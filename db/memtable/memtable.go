package memtable

import (
	"lsm/db/encoder"
	"lsm/db/skiplist"
)

type Memtable struct {
	sl        *skiplist.SkipList
	sizeUsed  int
	sizeLimit int
	encoder   *encoder.Encoder
}

func NewMemtable(sizeLimit int) *Memtable {
	m := &Memtable{
		sl:        skiplist.NewSkipList(),
		sizeUsed:  0,
		sizeLimit: sizeLimit,
		encoder:   encoder.NewEncoder(),
	}
	return m
}

func (m *Memtable) HasRoomForWrite(key, val []byte) bool {
	sizeNeeded := len(key) + len(val)
	return m.sizeUsed+sizeNeeded <= m.sizeLimit
}

func (m *Memtable) Insert(key, val []byte) {
	m.sl.Insert(key, m.encoder.Encode(encoder.OpTypeSet, val))
	m.sizeUsed += len(key) + len(val) + 1
}

func (m *Memtable) InsertTombstone(key []byte) {
	m.sl.Insert(key, m.encoder.Encode(encoder.OpTypeDelete, nil))
	m.sizeUsed += 1
}

func (m *Memtable) Get(key []byte) (*encoder.EncodedValue, error) {
	val, err := m.sl.Find(key)
	if err != nil {
		return nil, err
	}
	return m.encoder.Decode(val), nil
}

func (m *Memtable) Size() int {
	return m.sizeUsed
}

func (m *Memtable) Iterator() *skiplist.Iterator {
	return m.sl.Iterator()
}
