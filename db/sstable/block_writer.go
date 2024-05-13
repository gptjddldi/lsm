package sstable

import (
	"bytes"
	"encoding/binary"
	"math"
)

const (
	maxBlockSize      = 4 << 10
	offsetSizeInBytes = 4
)

var blockFlushThreshold = int(math.Floor(maxBlockSize * 0.9))

type blockWriter struct {
	buf *bytes.Buffer

	offsets      []uint32
	nextOffset   uint32
	trackOffsets bool
}

func newBlockWriter() *blockWriter {
	return &blockWriter{
		buf: bytes.NewBuffer(make([]byte, 0, maxBlockSize)),
	}
}

func (bw *blockWriter) scratchBuf(needed int) []byte {
	available := bw.buf.Available()
	if needed > available {
		bw.buf.Grow(needed)
	}
	buf := bw.buf.AvailableBuffer()
	return buf[:needed]
}

// *.sst file format:
// [keyLen:varInt][valLen:varInt][key:keyLen][encodedValue:valLen (OpType + value)]
func (bw *blockWriter) writeDataEntry(key, val []byte) (int, error) {
	keyLen, valLen := len(key), len(val)
	needed := 2*binary.MaxVarintLen64 + keyLen + valLen
	buf := bw.scratchBuf(needed)
	n := binary.PutUvarint(buf, uint64(keyLen))
	n += binary.PutUvarint(buf[n:], uint64(valLen))
	copy(buf[n:], key)
	copy(buf[n+keyLen:], val)
	used := n + keyLen + valLen
	n, err := bw.buf.Write(buf[:used])
	if err != nil {
		return n, err
	}
	if bw.trackOffsets {
		bw.trackOffset(uint32(n))
	}
	return n, nil
}

func (bw *blockWriter) trackOffset(n uint32) {
	bw.offsets = append(bw.offsets, bw.nextOffset)
	bw.nextOffset += n
}

func (bw *blockWriter) finish() error {
	if !bw.trackOffsets {
		return nil
	}
	numOffsets := len(bw.offsets)
	needed := (numOffsets + 2) * offsetSizeInBytes
	buf := bw.scratchBuf(needed)
	for i, offset := range bw.offsets {
		binary.LittleEndian.PutUint32(buf[i*offsetSizeInBytes:i*offsetSizeInBytes+offsetSizeInBytes], offset)
	}
	binary.LittleEndian.PutUint32(buf[needed-footerSizeInBytes:needed-offsetSizeInBytes], uint32(bw.buf.Len()+needed))
	binary.LittleEndian.PutUint32(buf[needed-offsetSizeInBytes:needed], uint32(numOffsets))
	_, err := bw.buf.Write(buf)
	if err != nil {
		return err
	}

	return nil
}
