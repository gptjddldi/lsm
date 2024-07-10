package lsm

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/gptjddldi/lsm/db/encoder"
	"github.com/gptjddldi/lsm/db/storage"
)

const (
	memtableSizeLimitBytes = 2 << 20 // 2MB
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

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
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
		compactionChan: make(chan int, 100),
		flushingChan:   make(chan *Memtable, 100),
		ctx:            ctx,
		cancel:         cancel,
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
	db.flushingChan <- db.memtables.mutable
	db.memtables.mutable = NewMemtable(memtableSizeLimitBytes)
	db.cancel()
	db.wg.Wait()

	close(db.flushingChan)
	close(db.compactionChan)
}

func (db *DB) doCompaction() {
	defer db.wg.Done()
	for {
		select {
		case <-db.ctx.Done():
			if readyToExit := db.checkAndTriggerCompaction(); readyToExit {
				return
			}
		case l := <-db.compactionChan:
			db.compactLevel(l)
		}
	}
}

func (db *DB) checkAndTriggerCompaction() bool {
	readyToExit := true

	if db.needL0Compaction() {
		db.compactionChan <- 0
		readyToExit = false
	}

	for idx := range db.levels {
		if idx == 0 {
			continue
		}
		if db.needLevelNCompaction(idx) {
			db.compactionChan <- idx
			readyToExit = false
		}
	}

	return readyToExit
}

func (db *DB) needL0Compaction() bool {
	return len(db.levels[0].sstables) > l0Capacity
}

func (db *DB) needLevelNCompaction(level int) bool {
	return db.levels[level].TotalSize() > calculateLevelSize(level)
}

func (db *DB) doFlushing() {
	defer db.wg.Done()
	for {
		select {
		case <-db.ctx.Done():
			return
		case m := <-db.flushingChan:
			db.flushMemtable(m)
		}
	}
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
		for i := len(db.levels[level].sstables) - 1; i >= 0; i-- {
			encodedValue, err := db.levels[level].sstables[i].Get(key)
			if err != nil {
				continue // Only NotFound error is expected
			}
			return db.handleEncodedValue(encodedValue)
		}
	}
	return nil, errors.New("key not found")
}

func (db *DB) handleEncodedValue(encodedValue *encoder.EncodedValue) ([]byte, error) {
	if encodedValue.IsTombstone() {
		return nil, errors.New("key not found")
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
	err = flusher.Flush()
	if err != nil {
		return err
	}
	sst, err := db.OpenSSTable(f)
	db.levels[0].sstables = append(db.levels[0].sstables, sst)
	db.memtables.queue = db.memtables.queue[1:]

	if db.needL0Compaction() {
		db.compactionChan <- 0
	}

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
	least := db.levels[level].sstables[0]
	for _, sstable := range db.levels[level].sstables {
		if sstable.file.Name() < least.file.Name() {
			least = sstable
		}
	}
	return least
}

func readEntry(reader *bufio.Reader) (*DataEntry, uint64) {
	keyLen, valLen := readEntryLengths(reader)
	key := make([]byte, keyLen)
	if keyLen == 0 {
		fmt.Println(123)
	}
	opType := make([]byte, 1)
	value := make([]byte, valLen-1)

	io.ReadFull(reader, key)
	io.ReadFull(reader, opType)
	io.ReadFull(reader, value)
	//fmt.Println("key", key, "val", value)
	de := &DataEntry{
		key:    key,
		value:  value,
		opType: encoder.OpType(opType[0]),
	}
	keyLenBytes := binary.PutUvarint(make([]byte, 10), keyLen)
	valLenBytes := binary.PutUvarint(make([]byte, 10), valLen)
	return de, keyLen + valLen + uint64(keyLenBytes) + uint64(valLenBytes)
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

func (db *DB) L0Compaction() {
	db.compactLevel(0)
}

func (db *DB) involvedIterators(level int, minKey, maxKey []byte) []*SSTableIterator {
	iterators := make([]*SSTableIterator, 0)
	for _, sstable := range db.levels[level].sstables {
		if sstable.IsInKeyRange(minKey, maxKey) {
			iterators = append(iterators, sstable.Iterator())
		}
	}
	return iterators
}
