package db

import (
	"errors"
	"log"
	"lsm/db/memtable"
	"lsm/db/sstable"
	"lsm/db/storage"
)

const (
	memtableSizeLimit      = 3 << 10
	memtableFlushThreshold = 4 << 10 // 4 * 2^10 = 4KB
)

type DB struct {
	dataStorage *storage.Provider
	memtables   struct {
		mutable *memtable.Memtable
		queue   []*memtable.Memtable // to be flushed
	}
	sstables []*storage.FileMetadata
}

func Open(dirname string) (*DB, error) {
	dataStorage, err := storage.NewProvider(dirname)
	if err != nil {
		return nil, err
	}

	db := &DB{
		dataStorage: dataStorage,
	}

	err = db.loadSSTables()
	if err != nil {
		return nil, err
	}

	db.memtables.mutable = memtable.NewMemtable(memtableSizeLimit)
	db.memtables.queue = append(db.memtables.queue, db.memtables.mutable)

	return db, nil
}

func (db *DB) loadSSTables() error {
	meta, err := db.dataStorage.ListFiles()
	if err != nil {
		return err
	}
	for _, f := range meta {
		if !f.IsSSTable() {
			continue
		}
		db.sstables = append(db.sstables, f)
	}
	return nil
}

func (db *DB) Insert(key, val []byte) {
	m := db.prepMemtableForKV(key, val)
	m.Insert(key, val)
	db.maybeFlushMemtables()
}

func (db *DB) rotateMemtables() *memtable.Memtable {
	db.memtables.queue = append(db.memtables.queue, db.memtables.mutable)
	db.memtables.mutable = memtable.NewMemtable(memtableSizeLimit)
	return db.memtables.mutable
}

func (db *DB) Get(key []byte) ([]byte, error) {
	for i := len(db.memtables.queue) - 1; i >= 0; i-- {
		m := db.memtables.queue[i]
		encodedValue, err := m.Get(key)
		if err != nil {
			continue
		} // Only NotFound error is expected
		if encodedValue.IsTombstone() {
			log.Printf(`Found key "%s" marked as deleted in memtable "%d".`, key, i)
			return nil, errors.New("key not found")
		}
		return encodedValue.Value(), nil
	}

	for i := len(db.sstables) - 1; i >= 0; i-- {
		meta := db.sstables[i]
		f, err := db.dataStorage.OpenFileForReading(meta)
		if err != nil {
			return nil, err
		}
		r := sstable.NewReader(f)
		encodedValue, err := r.Get(key)
		if err != nil {
			continue
		}
		if encodedValue.IsTombstone() {
			log.Printf(`Found key "%s" marked as deleted in sstable "%d".`, key, i)
			return nil, errors.New("key not found")
		}
		return encodedValue.Value(), nil
	}
	return nil, errors.New("key not found")
}

func (db *DB) Delete(key []byte) {
	m := db.prepMemtableForKV(key, nil)
	m.InsertTombstone(key)
	db.maybeFlushMemtables()
}

func (db *DB) prepMemtableForKV(key, val []byte) *memtable.Memtable {
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
		w := sstable.NewWriter(f)
		err = w.Process(flushable[i])
		if err != nil {
			return err
		}

		err = w.Close()
		if err != nil {
			return err
		}
		db.sstables = append(db.sstables, meta)
	}
	return nil
}
