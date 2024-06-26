package lsm

import (
	"github.com/gptjddldi/lsm/db/encoder"
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
	key, val := iterator.Current()
	entry := &DataEntry{
		key:    key,
		value:  val[1:],
		opType: encoder.OpType(val[0]),
	}
	de = append(de, entry)
	for iterator.HasNext() {
		key, val = iterator.Next()
		entry := &DataEntry{
			key:    key,
			value:  val[1:],
			opType: encoder.OpType(val[0]),
		}
		de = append(de, entry)
	}
	f.writer.Write(de)
	return nil
}
