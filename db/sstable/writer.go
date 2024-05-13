package sstable

import (
	"bufio"
	"encoding/binary"
	"io"
	"lsm/db/encoder"
	"lsm/db/memtable"
)

type syncCloser interface {
	io.Closer
	Sync() error
}

type Writer struct {
	file       syncCloser
	bw         *bufio.Writer
	buf        []byte
	dataBlock  *blockWriter
	indexBlock *blockWriter

	encoder *encoder.Encoder

	offset       int
	bytesWritten int
	lastKey      []byte
}

func NewWriter(file io.Writer) *Writer {
	w := &Writer{}
	bw := bufio.NewWriter(file)
	w.file, w.bw = file.(syncCloser), bw
	w.buf = make([]byte, 0, 1024)
	w.dataBlock, w.indexBlock = newBlockWriter(), newBlockWriter()
	w.indexBlock.trackOffsets = true
	return w
}

func (w *Writer) Process(m *memtable.Memtable) error {
	i := m.Iterator()
	for i.HasNext() {
		key, val := i.Next()
		n, err := w.dataBlock.writeDataEntry(key, val)
		if err != nil {
			return err
		}
		w.bytesWritten += n
		w.lastKey = key

		if w.bytesWritten > blockFlushThreshold {
			err = w.flushDataBlock()
			if err != nil {
				return err
			}
		}
	}
	err := w.flushDataBlock()
	if err != nil {

		return err
	}
	err = w.indexBlock.finish()
	if err != nil {
		return err
	}
	_, err = w.bw.ReadFrom(w.indexBlock.buf)
	return err
}

func (w *Writer) Close() error {
	err := w.bw.Flush()
	if err != nil {
		return err
	}

	err = w.file.Sync()
	if err != nil {
		return err
	}

	err = w.file.Close()
	if err != nil {
		return err
	}

	w.bw = nil
	w.file = nil

	return err
}

func (w *Writer) flushDataBlock() error {
	if w.bytesWritten <= 0 {
		return nil
	}
	n, err := w.bw.ReadFrom(w.dataBlock.buf)
	if err != nil {
		return err
	}
	err = w.addIndexEntry()
	if err != nil {
		return err
	}
	w.offset += int(n)
	w.bytesWritten = 0
	return nil
}

func (w *Writer) addIndexEntry() error {
	buf := w.buf[:8]
	binary.LittleEndian.PutUint32(buf[:4], uint32(w.offset))
	binary.LittleEndian.PutUint32(buf[4:], uint32(w.bytesWritten))
	_, err := w.indexBlock.writeDataEntry(w.lastKey, w.encoder.Encode(encoder.OpTypeSet, buf))
	if err != nil {
		return err
	}
	return nil
}
