package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"lsm/db/memtable"
)

type syncCloser interface {
	io.Closer
	Sync() error
}

type Writer struct {
	file       syncCloser
	bw         *bufio.Writer
	buf        *bytes.Buffer
	offsets    []uint32
	nextOffset uint32
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
		n, err := w.writeDataBlock(key, val)
		if err != nil {
			return err
		}
		w.addIndexEntry(n)
	}
	err := w.writeIndexBlock()
	if err != nil {
		return err
	}
	return nil
}

// *.sst file format:
// [keyLen:2][valLen:2][key:keyLen][encodedValue:valLen (OpType + value)]
func (w *Writer) writeDataBlock(key, val []byte) (int, error) {
	keyLen, valLen := len(key), len(val)
	needed := 2*binary.MaxVarintLen64 + keyLen + valLen
	buf := w.scratchBuf(needed)
	n := binary.PutUvarint(buf, uint64(keyLen))
	n += binary.PutUvarint(buf[n:], uint64(valLen))
	copy(buf[n:], key)
	copy(buf[n+keyLen:], val)
	used := n + keyLen + valLen
	_, err := w.buf.Write(buf[:used])
	if err != nil {
		return n, err
	}
	m, err := w.bw.ReadFrom(w.buf)
	if err != nil {
		return int(m), err
	}
	return int(m), nil
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

func (w *Writer) scratchBuf(needed int) []byte {
	available := w.buf.Available()
	if needed > available {
		w.buf.Grow(needed)
	}
	buf := w.buf.AvailableBuffer()
	return buf[:needed]
}

func (w *Writer) addIndexEntry(n int) {
	w.offsets = append(w.offsets, w.nextOffset)
	w.nextOffset += uint32(n)
}

func (w *Writer) writeIndexBlock() error {
	numOffsets := len(w.offsets)
	needed := (numOffsets + 1) * 4
	buf := w.scratchBuf(needed)
	for i, offset := range w.offsets {
		binary.LittleEndian.PutUint32(buf[i*4:i*4+4], offset)
	}
	binary.LittleEndian.PutUint32(buf[needed-4:needed], uint32(numOffsets))
	_, err := w.bw.Write(buf[:])
	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}
