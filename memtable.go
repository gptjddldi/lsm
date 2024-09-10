package lsm

import (
	"github.com/gptjddldi/lsm/db/encoder"
	"github.com/gptjddldi/lsm/db/skiplist"
)

type Memtable struct {
	sl        *skiplist.SkipList
	sizeUsed  int
	sizeLimit int
}

func NewMemtable(sizeLimit int, useLearnedIndex bool) *Memtable {
	m := &Memtable{
		sl:        skiplist.NewSkipList(useLearnedIndex),
		sizeUsed:  0,
		sizeLimit: sizeLimit,
	}
	return m
}

func (m *Memtable) HasRoomForWrite(key, val []byte) bool {
	sizeNeeded := len(key) + len(val)
	return m.sizeUsed+sizeNeeded <= m.sizeLimit
}

func (m *Memtable) Insert(key, val []byte) {
	m.sl.Insert(key, encoder.Encode(encoder.OpTypeSet, val))
	m.sizeUsed += len(key) + len(val) + 1
}

func (m *Memtable) InsertTombstone(key []byte) {
	m.sl.Insert(key, encoder.Encode(encoder.OpTypeDelete, nil))
	m.sizeUsed += 1
}

func (m *Memtable) Get(key []byte) (*encoder.EncodedValue, error) {
	val, err := m.sl.Find(key)
	if err != nil {
		return nil, err
	}
	return encoder.Decode(val), nil
}

func (m *Memtable) Size() int {
	return m.sizeUsed
}

func (m *Memtable) Iterator() *skiplist.Iterator {
	return m.sl.Iterator()
}
