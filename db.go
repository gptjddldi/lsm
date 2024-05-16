package lsm

import (
	"errors"
	"github.com/gptjddldi/lsm/db/storage"
	"log"
)

const (
	memtableSizeLimit      = 5 * 3 << 10
	memtableFlushThreshold = 1
)

type DataEntry struct {
	key   []byte
	value []byte
}

type DB struct {
	dataStorage *storage.Provider
	memtables   struct {
		mutable *Memtable
		queue   []*Memtable // to be flushed
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

	db.memtables.mutable = NewMemtable(memtableSizeLimit)
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

		r, err := NewReader(f)
		if err != nil {
			return nil, err
		}
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

//func (db *DB) Test() error {
//	s1 := db.sstables[0]
//	f, err := db.dataStorage.OpenFileForReading(s1)
//	if err != nil {
//		return err
//	}
//	sstable := NewSSTable(f)
//	it := sstable.Iterator()
//	for {
//		ok, err := it.Next()
//		if err != nil {
//			return err
//		}
//		if !ok {
//			break
//		}
//
//		entry := it.entry
//		fmt.Printf("Key: %s, Value: %s\n", entry.key, entry.value)
//	}
//	return nil
//}

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

		//w := sstable.NewWriter(f)
		//err = w.Process(flushable[i])
		//if err != nil {
		//	return err
		//}
		//
		//err = w.Close()
		//if err != nil {
		//	return err
		//}
		db.sstables = append(db.sstables, meta)
	}
	return nil
}

//func (db *DB) compact() error {
//	meta1 := db.sstables[0]
//	meta2 := db.sstables[1]
//		f, err := db.dataStorage.OpenFileForReading(meta)
//		if err != nil {
//			return nil, err
//		}
//
//		r, err := sstable.NewReader(f)
//		if err != nil {
//			return nil, err
//		}
//		encodedValue, err := r.Get(key)
//		if err != nil {
//			continue
//		}
//		if encodedValue.IsTombstone() {
//			log.Printf(`Found key "%s" marked as deleted in sstable "%d".`, key, i)
//			return nil, errors.New("key not found")
//		}
//		return encodedValue.Value(), nil
//	}
//
//}
