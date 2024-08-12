package lsm

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/gptjddldi/lsm/db/encoder"
	"github.com/gptjddldi/lsm/db/storage"
)

const (
	memtableSizeLimitBytes = 10 << 20 // 100MB
)

var ErrorKeyNotFound = errors.New("key not found")

type DataEntry struct {
	key    []byte
	value  []byte
	opType encoder.OpType
}

type DB struct {
	dataStorage *storage.Provider
	memtables   struct {
		mutable *Memtable
		queue   []*Memtable // to be flushed
	}
	flushingChan chan *Memtable
	levels       []*level

	compactionChan chan int

	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	isCompacting []bool
	compactionMu sync.RWMutex
}

func Open(dirname string) (*DB, error) {
	ctx, cancel := context.WithCancel(context.Background())

	dataStorage, err := storage.NewProvider(dirname)
	if err != nil {
		cancel()
		return nil, err
	}

	db := &DB{
		dataStorage:    dataStorage,
		compactionChan: make(chan int, 1000),
		flushingChan:   make(chan *Memtable, 1000),
		ctx:            ctx,
		cancel:         cancel,
		isCompacting:   make([]bool, maxLevel),
	}

	levels := make([]*level, maxLevel)
	for i := 0; i < maxLevel; i++ {
		levels[i] = &level{
			sstables: make([]*SSTable, 0),
		}
	}
	db.levels = levels
	err = db.loadSSTFilesFromDisk()
	if err != nil {
		return nil, err
	}
	db.memtables.mutable = NewMemtable(memtableSizeLimitBytes)
	db.memtables.queue = append(db.memtables.queue, db.memtables.mutable)

	db.wg.Add(2)
	go db.doCompaction()
	go db.doFlushing()

	return db, nil
}

func (db *DB) Close() {
	// Flush the current mutable memtable
	db.flushingChan <- db.memtables.mutable
	db.memtables.mutable = NewMemtable(memtableSizeLimitBytes)

	// Trigger final compactions
	db.checkAndTriggerCompaction()

	// Signal all goroutines to stop and process remaining tasks
	db.cancel()

	// Wait for all goroutines to finish
	db.wg.Wait()

	// Close channels
	close(db.flushingChan)
	close(db.compactionChan)
}

func (db *DB) doCompaction() {
	defer db.wg.Done()
	for {
		select {
		case <-db.ctx.Done():
			db.processRemainingCompactions()
			return
		case l := <-db.compactionChan:
			if db.needLevelNCompaction(l) {
				db.processCompaction(l)
			}
		}
	}
}

func (db *DB) processRemainingCompactions() {
	for len(db.compactionChan) > 0 {
		l := <-db.compactionChan
		if db.needLevelNCompaction(l) {
			db.processCompaction(l)
		}
	}
}

func (db *DB) processCompaction(l int) {
	fmt.Println("compaction level", l)
	db.compactionMu.Lock()
	if db.isCompacting[l] {
		db.compactionMu.Unlock()
		return
	}
	db.isCompacting[l] = true
	db.compactionMu.Unlock()

	defer func() {
		db.compactionMu.Lock()
		db.isCompacting[l] = false
		db.compactionMu.Unlock()
	}()

	if err := db.compactLevel(l); err != nil {
		log.Printf("Error compacting level %d: %v", l, err)
	}
}

func (db *DB) doFlushing() {
	defer db.wg.Done()
	for {
		select {
		case <-db.ctx.Done():
			db.processRemainingFlushes()
			return
		case m := <-db.flushingChan:
			db.processFlush(m)
		}
	}
}

func (db *DB) processRemainingFlushes() {
	for len(db.flushingChan) > 0 {
		m := <-db.flushingChan
		db.processFlush(m)
	}
}

func (db *DB) processFlush(m *Memtable) {
	if err := db.flushMemtable(m); err != nil {
		log.Printf("Error flushing memtable: %v", err)
	}
}

func (db *DB) checkAndTriggerCompaction() bool {
	readyToExit := true

	for idx := range db.levels {
		if db.needLevelNCompaction(idx) {
			db.compactionChan <- idx
			readyToExit = false
		}
	}
	return readyToExit
}

func (db *DB) Insert(key, val []byte) {
	db.prepMemtableForKV(key, val)
	db.memtables.mutable.Insert(key, val)
}

func (db *DB) Get(key []byte) ([]byte, error) {
	for i := len(db.memtables.queue) - 1; i >= 0; i-- {
		m := db.memtables.queue[i]
		encodedValue, err := m.Get(key)
		if err != nil {
			continue
		} // Only NotFound error is expected
		return db.handleEncodedValue(encodedValue)
	}
	for level := range db.levels {
		for _, sstable := range db.levels[level].sstables {
			encodedValue, err := sstable.Get(key)
			if err != nil {
				continue // Only NotFound error is expected
			}
			return db.handleEncodedValue(encodedValue)
		}
	}
	return nil, ErrorKeyNotFound
}

func (db *DB) handleEncodedValue(encodedValue *encoder.EncodedValue) ([]byte, error) {
	if encodedValue.IsTombstone() {
		return nil, ErrorKeyNotFound
	}
	return encodedValue.Value(), nil
}

func (db *DB) Delete(key []byte) {
	db.prepMemtableForKV(key, nil)
	db.memtables.mutable.InsertTombstone(key)
}

func (db *DB) prepMemtableForKV(key, val []byte) {
	if !db.memtables.mutable.HasRoomForWrite(key, val) {
		db.memtables.queue = append(db.memtables.queue, db.memtables.mutable)
		db.flushingChan <- db.memtables.mutable
		m := NewMemtable(memtableSizeLimitBytes)
		db.memtables.mutable = m
	}
}

func (db *DB) flushMemtable(m *Memtable) error {
	meta := db.dataStorage.PrepareNewFile(0)
	f, err := db.dataStorage.OpenFileForWriting(meta)
	if err != nil {
		return err
	}

	flusher := NewFlusher(m, f)
	if err = flusher.Flush(); err != nil {
		return err
	}

	sst, err := db.OpenSSTable(f)
	if err != nil {
		return err
	}

	db.levels[0].sstables = append(db.levels[0].sstables, sst)
	db.memtables.queue = db.memtables.queue[1:]

	db.compactionChan <- 0

	return nil
}

func (db *DB) loadSSTFilesFromDisk() error {
	files, err := db.dataStorage.ListFiles()
	if err != nil {
		return err
	}
	for _, f := range files {
		if !f.IsSSTable() {
			continue
		}
		sst, err := db.OpenSSTableByFileName(f.Path())
		if err != nil {
			return err
		}
		db.levels[f.Level()].sstables = append(db.levels[f.Level()].sstables, sst)
	}
	return nil
}

func (db *DB) OpenSSTable(file *os.File) (*SSTable, error) {
	return NewSSTable(file)
}

// todo: can be improved
func (db *DB) deleteSStableAtLevel(level int, iterators []*SSTableIterator) {
	iteratorMap := make(map[*SSTable]bool)
	for _, iterator := range iterators {
		iteratorMap[iterator.sstable] = true
	}

	newSSTables := make([]*SSTable, 0)
	for _, sstable := range db.levels[level].sstables {
		if iteratorMap[sstable] {
			if err := os.Remove(sstable.file.Name()); err != nil {
				log.Printf("Error deleting file: %v", err)
			}
		} else {
			newSSTables = append(newSSTables, sstable)
		}
	}

	db.levels[level].sstables = newSSTables
}

func (db *DB) LeastSstableAtLevel(level int) *SSTable {
	if len(db.levels[level].sstables) == 0 {
		return nil
	}

	if level == 1 {
		// For level 1, pick a random SSTable
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		randomIndex := r.Intn(len(db.levels[level].sstables))
		return db.levels[level].sstables[randomIndex]
	}

	// For other levels, keep the existing logic
	least := db.levels[level].sstables[0]
	for _, sstable := range db.levels[level].sstables {
		if sstable.file.Name() < least.file.Name() {
			least = sstable
		}
	}
	return least
}

func readEntry(reader *bufio.Reader) (*DataEntry, uint64, error) {
	keyLen, valLen := readEntryLengths(reader)
	if keyLen == 0 {
		return nil, 0, fmt.Errorf("invalid key length: 0")
	}

	key := make([]byte, keyLen)
	opType := make([]byte, 1)
	value := make([]byte, valLen-1)

	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, 0, fmt.Errorf("error reading key: %w", err)
	}
	if _, err := io.ReadFull(reader, opType); err != nil {
		return nil, 0, fmt.Errorf("error reading opType: %w", err)
	}
	if _, err := io.ReadFull(reader, value); err != nil {
		return nil, 0, fmt.Errorf("error reading value: %w", err)
	}

	de := &DataEntry{
		key:    key,
		value:  value,
		opType: encoder.OpType(opType[0]),
	}

	totalBytes := keyLen + valLen
	keyLenBytes := binary.PutUvarint(make([]byte, binary.MaxVarintLen64), keyLen)
	valLenBytes := binary.PutUvarint(make([]byte, binary.MaxVarintLen64), valLen)
	totalBytes += uint64(keyLenBytes + valLenBytes)

	return de, totalBytes, nil
}

func readEntryLengths(reader *bufio.Reader) (uint64, uint64) {
	keyLen, _ := binary.ReadUvarint(reader)
	valLen, _ := binary.ReadUvarint(reader)
	return keyLen, valLen
}

func (db *DB) OpenSSTableByFileName(fileName string) (*SSTable, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	return db.OpenSSTable(file)
}
