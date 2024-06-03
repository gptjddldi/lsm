package lsm

import (
	"context"
	"errors"
	"github.com/gptjddldi/lsm/db/encoder"
	"github.com/gptjddldi/lsm/db/storage"
	"log"
	"os"
)

const (
	memtableSizeLimit      = 5 * 3 << 10
	memtableFlushThreshold = 1
	maxLevel               = 6
)

var maxLevelSSTables = map[int]int{
	0: 4,
	1: 8,
	2: 16,
	3: 32,
	4: 64,
	5: 128,
	6: 256,
}

var ErrorKeyNotFound = errors.New("key not found")

type DataEntry struct {
	key   []byte
	value []byte
}

type level struct {
	sstables []*SSTable // SSTables in this level
}

type DB struct {
	dataStorage *storage.Provider
	memtables   struct {
		mutable *Memtable
		queue   []*Memtable // to be flushed
	}
	levels []*level

	compactionChan chan int

	ctx    context.Context
	cancel context.CancelFunc
}

func Open(dirname string) (*DB, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dataStorage, err := storage.NewProvider(dirname)
	if err != nil {
		return nil, err
	}

	db := &DB{
		dataStorage:    dataStorage,
		compactionChan: make(chan int, 100),
		ctx:            ctx,
		cancel:         cancel,
	}

	levels := make([]*level, maxLevel)
	for i := 0; i < maxLevel; i++ {
		levels[i] = &level{
			sstables: make([]*SSTable, 0),
		}
	}
	err = db.loadSSTFilesFromDisk()
	if err != nil {
		return nil, err
	}
	db.levels = levels
	db.memtables.mutable = NewMemtable(memtableSizeLimit)
	db.memtables.queue = append(db.memtables.queue, db.memtables.mutable)

	go db.doCompaction()

	return db, nil
}

func (db *DB) doCompaction() {
	for {
		select {
		case <-db.ctx.Done():
			if readyToExit := db.checkAndTriggerCompaction(); readyToExit {
				return
			}
		case <-db.compactionChan:
			db.compactLevel(0)
		}
	}
}

func (db *DB) checkAndTriggerCompaction() bool {
	readyToExit := true
	for idx, level := range db.levels {
		if len(level.sstables) > maxLevelSSTables[idx] {
			db.compactionChan <- idx
			readyToExit = false
		}
	}
	return readyToExit
}

func (db *DB) Insert(key, val []byte) {
	m := db.prepMemtableForKV(key, val)
	m.Insert(key, val)
	db.maybeFlushMemtables()
}

func (db *DB) rotateMemtables() *Memtable {
	db.memtables.queue = append(db.memtables.queue, db.memtables.mutable)
	db.memtables.mutable = NewMemtable(memtableSizeLimit)
	return db.memtables.mutable
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
	m := db.prepMemtableForKV(key, nil)
	m.InsertTombstone(key)
	db.maybeFlushMemtables()
}

func (db *DB) prepMemtableForKV(key, val []byte) *Memtable {
	m := db.memtables.mutable
	if !db.memtables.mutable.HasRoomForWrite(key, val) {
		m = db.rotateMemtables()
	}
	return m
}

func (db *DB) maybeFlushMemtables() {
	var totalSize int
	for i := 0; i < len(db.memtables.queue); i++ {
		totalSize += db.memtables.queue[i].Size()
	}
	if totalSize < memtableFlushThreshold {
		return
	}

	err := db.flushMemtables()
	if err != nil {
		log.Printf("Error flushing memtables: %v", err)
	}
}

func (db *DB) flushMemtables() error {
	n := len(db.memtables.queue) - 1
	flushable := db.memtables.queue[:n]
	db.memtables.queue = db.memtables.queue[n:]

	for i := 0; i < len(flushable); i++ {
		meta := db.dataStorage.PrepareNewFile()
		f, err := db.dataStorage.OpenFileForWriting(meta)
		if err != nil {
			return err
		}

		flusher := NewFlusher(flushable[i], f)
		err = flusher.Flush()
		if err != nil {
			return err
		}
		sst, err := OpenSSTable(f.Name())
		db.levels[0].sstables = append(db.levels[0].sstables, sst)
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
		sst, err := OpenSSTable(f.Name())
		if err != nil {
			return err
		}
		db.levels[f.Level()].sstables = append(db.levels[f.Level()].sstables, sst)
	}
	return nil
}

func OpenSSTable(filename string) (*SSTable, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	sst := NewSSTable(file)
	return sst, nil
}
