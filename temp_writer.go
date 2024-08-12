package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"math"
	"os"

	"github.com/gptjddldi/lsm/db/encoder"
)

const (
	maxBlockSize = 4 << 10
)

var BlockThreshold = int(math.Floor(maxBlockSize * 0.9))

type TempWriter struct {
	bw *bufio.Writer

	indexLength  int
	curOffset    int
	nextOffset   uint32
	writtenBytes int

	dataBlockBuf *bytes.Buffer
	indexBuf     *bytes.Buffer
	BloomFilter  *BloomFilter
	lastKey      []byte

	footerBuf *bytes.Buffer
	offsets   []uint32
}

func NewTempWriter(file *os.File) *TempWriter {
	return &TempWriter{
		dataBlockBuf: bytes.NewBuffer(make([]byte, 0, maxBlockSize)),
		bw:           bufio.NewWriter(file),
		indexBuf:     bytes.NewBuffer(make([]byte, 0, maxBlockSize)),
		BloomFilter:  NewBloomFilter(),
		footerBuf:    bytes.NewBuffer(make([]byte, 0, 8)),
	}
}

// compaction / flush 시 호출
func (tw *TempWriter) Write(entries []*DataEntry) error {
	for _, entry := range entries {
		tw.BloomFilter.Add(entry.key)
		n, err := tw.dataBlockBuf.Write(entry.toBytes())
		if err != nil {
			return err
		}
		tw.writtenBytes += n
		tw.lastKey = entry.key
		if tw.writtenBytes > BlockThreshold {
			err := tw.flushDataBlock()
			if err != nil {
				return err
			}
		}
	}
	err := tw.flushDataBlock()
	if err != nil {
		return err
	}

	indexLen, err := tw.bw.ReadFrom(tw.indexBuf)
	if err != nil {
		return err
	}

	bloomFilterBuf := &bytes.Buffer{}
	_, err = tw.BloomFilter.filter.WriteTo(bloomFilterBuf)
	if err != nil {
		return err
	}
	bloomFilterLen, err := tw.bw.ReadFrom(bloomFilterBuf)
	if err != nil {
		return err
	}

	footer := tw.buildFooterBlock(indexLen, bloomFilterLen)
	tw.footerBuf.Write(footer)
	_, err = tw.bw.ReadFrom(tw.footerBuf)
	if err != nil {
		return err
	}

	return nil
}

func (tw *TempWriter) flushDataBlock() error {
	if tw.writtenBytes == 0 {
		return nil
	}
	n, err := tw.bw.ReadFrom(tw.dataBlockBuf)
	if err != nil {
		return err
	}

	entry := tw.buildIndexEntry()
	_, err = tw.indexBuf.Write(entry)
	if err != nil {
		return err
	}
	tw.writtenBytes = 0
	tw.curOffset += int(n)
	return nil
}

func (tw *TempWriter) buildIndexEntry() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], uint32(tw.curOffset))
	binary.LittleEndian.PutUint32(buf[4:], uint32(tw.writtenBytes))
	entry := &DataEntry{
		key:    tw.lastKey,
		value:  buf,
		opType: encoder.OpTypeSet,
	}
	return entry.toBytes()
}

// footer block 에 넣어야 하는 건 index block 의 길이와 bloom filter 의 길이
func (tw *TempWriter) buildFooterBlock(indexLen, bloomLen int64) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[:8], uint64(indexLen))
	binary.LittleEndian.PutUint64(buf[8:], uint64(bloomLen))
	return buf
}

// { key length, value length, key, (opKind, value) }
func (de *DataEntry) toBytes() []byte {
	key, val, opType := de.key, de.value, de.opType

	keyLen, valLen := len(key), len(val)+1
	needed := 2*binary.MaxVarintLen64 + keyLen + valLen

	buf := make([]byte, needed)
	n := binary.PutUvarint(buf, uint64(keyLen))
	n += binary.PutUvarint(buf[n:], uint64(valLen))
	copy(buf[n:], key)
	copy(buf[n+keyLen:], encoder.Encode(opType, val))
	used := n + keyLen + valLen

	return buf[:used]
}
