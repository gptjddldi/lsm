package db

import (
	"errors"
	"log"
	"lsm/memtable"
	"lsm/sstable"
	"lsm/storage"
)

const (
	memtableSizeLimit = 4 << 10
)

type DB struct {
	dataStorage *storage.Provider
	memtables   struct {
		mutable *memtable.Memtable
		queue   []*memtable.Memtable // to be flushed
	}
}

func Open(dirname string) (*DB, error) {
	dataStorage, err := storage.NewProvider(dirname)
	if err != nil {
		return nil, err
	}

	db := &DB{
		dataStorage: dataStorage,
	}
	db.memtables.mutable = memtable.NewMemtable(memtableSizeLimit)
	db.memtables.queue = append(db.memtables.queue, db.memtables.mutable)

	return db, nil
}

func (db *DB) Insert(key, val []byte) {
	m := db.prepMemtableForKV(key, val)
	m.Insert(key, val)
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
		}
		if encodedValue.IsTombstone() {
			log.Printf(`Found key "%s" marked as deleted in memtable "%d".`, key, i)
			return nil, errors.New("key not found")
		}
		return encodedValue.Value(), nil
	}
	return nil, errors.New("key not found")
}

func (db *DB) Delete(key []byte) {
	m := db.prepMemtableForKV(key, nil)
	m.InsertTombstone(key)
}

func (db *DB) prepMemtableForKV(key, val []byte) *memtable.Memtable {
	m := db.memtables.mutable
	if !db.memtables.mutable.HasRoomForWrite(key, val) {
		m = db.rotateMemtables()
	}
	return m
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
	}
	return nil
}
