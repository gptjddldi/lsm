package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/gptjddldi/lsm/db/encoder"
	"math"
	"os"
)

const (
	maxBlockSize      = 4 << 10
	offsetSizeInBytes = 4
)

var tempBlockThreshold = int(math.Floor(maxBlockSize * 0.9))

type TempWriter struct {
	buf  *bytes.Buffer
	file *os.File
	bw   *bufio.Writer

	indexLength  int
	curOffset    int
	nextOffset   uint32
	writtenBytes int

	indexBuf *bytes.Buffer
	lastKey  []byte

	footerBuf *bytes.Buffer

	encoder *encoder.Encoder
}

func NewTempWriter(file *os.File) *TempWriter {
	return &TempWriter{
		buf:       bytes.NewBuffer(make([]byte, 0, maxBlockSize)),
		file:      file,
		bw:        bufio.NewWriter(file),
		indexBuf:  bytes.NewBuffer(make([]byte, 0, maxBlockSize)),
		footerBuf: bytes.NewBuffer(make([]byte, 0, 8)),
	}
}

// compaction / flush 시 호출
func (tw *TempWriter) Write(entries []*DataEntry) error {
	for _, entry := range entries {
		key := entry.key
		value := entry.value
		buf := tw.buildEntry(key, value)
		tw.growIfNeeded(len(buf), tw.buf)
		n, err := tw.buf.Write(buf)
		if err != nil {
			return err
		}
		tw.writtenBytes += n
		tw.lastKey = key
		if tw.writtenBytes > tempBlockThreshold {
			err := tw.flushDataBlock()
			if err != nil {
				return err
			}
		}
	}
	footer := tw.buildFooterBlock()

	err := tw.flushDataBlock()
	if err != nil {
		return err
	}

	_, err = tw.bw.ReadFrom(tw.indexBuf)
	if err != nil {
		return err
	}

	tw.growIfNeeded(len(footer), tw.footerBuf)
	tw.footerBuf.Write(footer)
	_, err = tw.bw.ReadFrom(tw.footerBuf)
	if err != nil {
		return err
	}

	return nil
}

// data entry or index entry
// { key length, value length, key, (opKind, value) }
func (tw *TempWriter) buildEntry(key, val []byte) []byte {
	keyLen, valLen := len(key), len(val)
	needed := 2*binary.MaxVarintLen64 + keyLen + valLen

	buf := make([]byte, needed)
	n := binary.PutUvarint(buf, uint64(keyLen))
	n += binary.PutUvarint(buf[n:], uint64(valLen))
	copy(buf[n:], key)
	copy(buf[n+keyLen:], val)
	used := n + keyLen + valLen

	return buf[:used]
}

func (tw *TempWriter) flushDataBlock() error {
	if tw.writtenBytes <= 0 {
		return nil
	}
	n, err := tw.bw.ReadFrom(tw.buf)
	if err != nil {
		return err
	}

	entry := tw.buildIndexEntry()
	tw.growIfNeeded(len(entry), tw.indexBuf)
	tw.indexBuf.Write(entry)
	tw.writtenBytes = 0
	tw.curOffset += int(n)
	return nil
}

func (tw *TempWriter) buildIndexEntry() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], uint32(tw.curOffset))
	binary.LittleEndian.PutUint32(buf[4:], uint32(tw.writtenBytes))
	tw.indexLength += 1
	return tw.buildEntry(tw.lastKey, tw.encoder.Encode(encoder.OpTypeSet, buf))
}

func (tw *TempWriter) buildFooterBlock() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], uint32(tw.indexBuf.Len()+8))
	binary.LittleEndian.PutUint32(buf[4:], uint32(tw.indexLength))
	return buf
}

func (tw *TempWriter) growIfNeeded(needed int, buf *bytes.Buffer) {
	available := buf.Available()
	if needed > available {
		buf.Grow(needed)
	}
}

func (tw *TempWriter) scratchBuf(needed int) []byte {
	available := tw.buf.Available()
	if needed > available {
		tw.buf.Grow(needed)
	}
	buf := tw.buf.AvailableBuffer()
	return buf[:needed]
}
