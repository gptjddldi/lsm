package lsm

import (
	"os"
)

type Flusher struct {
	memtable *Memtable
	file     *os.File
	writer   *TempWriter
}

func NewFlusher(memtable *Memtable, file *os.File) *Flusher {
	return &Flusher{
		memtable: memtable,
		file:     file,
		writer:   NewTempWriter(file),
	}
}

func (f *Flusher) Flush() error {
	de := make([]*DataEntry, 0, 500)
	iterator := f.memtable.Iterator()
	for iterator.HasNext() {
		key, val := iterator.Next()
		entry := &DataEntry{
			key:   key,
			value: val,
		}
		de = append(de, entry)
	}
	f.writer.Write(de)
	return nil
}
