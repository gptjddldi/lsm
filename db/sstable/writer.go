package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"lsm/db/memtable"
)

type syncCloser interface {
	io.Closer
	Sync() error
}

type Writer struct {
	file syncCloser
	bw   *bufio.Writer
	buf  *bytes.Buffer
}

func NewWriter(file io.Writer) *Writer {
	bw := bufio.NewWriter(file)
	w := &Writer{
		file: file.(syncCloser),
		bw:   bw,
		buf:  bytes.NewBuffer(make([]byte, 0, 1024)),
	}
	return w
}

func (w *Writer) Process(m *memtable.Memtable) error {
	i := m.Iterator()
	for i.HasNext() {
		key, val := i.Next()
		err := w.writeDataBlock(key, val)

		if err != nil {
			return err
		}
	}
	return nil
}

// *.sst file format:
// [keyLen:2][valLen:2][key:keyLen][encodedValue:valLen (OpType + value)]
func (w *Writer) writeDataBlock(key, val []byte) error {
	keyLen, valLen := len(key), len(val)
	needed := 2*binary.MaxVarintLen64 + keyLen + valLen
	available := w.buf.Available()
	if needed > available {
		w.buf.Grow(needed)
	}
	buf := w.buf.AvailableBuffer()
	buf = buf[:needed]
	n := binary.PutUvarint(buf, uint64(keyLen))
	n += binary.PutUvarint(buf[n:], uint64(valLen))
	copy(buf[n:], key)
	copy(buf[n+keyLen:], val)
	used := n + keyLen + valLen
	_, err := w.buf.Write(buf[:used])
	if err != nil {
		return err
	}
	_, err = w.bw.ReadFrom(w.buf)
	if err != nil {
		return err
	}
	return nil
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
