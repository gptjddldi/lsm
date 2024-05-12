package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
)

const (
	tempMaxBlockSize = 4 << 10
)

var tempBlockThreshold = int(math.Floor(tempMaxBlockSize * 0.9))

type TempWriter struct {
	buf  *bytes.Buffer
	file *os.File
	bw   *bufio.Writer

	offsets      []uint32
	nextOffset   uint32
	writtenBytes int
}
type DataEntry struct {
	Key   []byte
	Value []byte
}

func NewTempWriter(file *os.File) *TempWriter {
	return &TempWriter{
		buf:  bytes.NewBuffer(make([]byte, 0, tempMaxBlockSize)),
		file: file,
		bw:   bufio.NewWriter(file),
	}
}

// sst 파일에 쓰는 부분
// compaction / flush 시에 호출
// data entry 받아서 data block / index block / meta block 생성해서 써줌
func (tw *TempWriter) Write(entries []*DataEntry) error {
	for _, entry := range entries {
		n, err := tw.writeDataEntry(entry.Key, entry.Value)
		if err != nil {
			return err
		}
		tw.writtenBytes += n

		if tw.writtenBytes > tempBlockThreshold {
			fmt.Println("hihihihihihihihi")
		}
	}
	return nil
}

func (tw *TempWriter) writeDataEntry(key, val []byte) (int, error) {
	keyLen, valLen := len(key), len(val)
	needed := 2*binary.MaxVarintLen64 + keyLen + valLen
	buf := tw.scratchBuf(needed)
	n := binary.PutUvarint(buf, uint64(keyLen))
	n += binary.PutUvarint(buf[n:], uint64(valLen))
	copy(buf[n:], key)
	copy(buf[n+keyLen:], val)
	used := n + keyLen + valLen

	n, err := tw.buf.Write(buf[:used])
	if err != nil {
		return n, err
	}

	tw.trackOffset(uint32(n))
	return n, nil
}

func (tw *TempWriter) buildIndexBlock() []byte {
	offsetLength := len(tw.offsets)
	needed := offsetLength * 4
	buffer := tw.scratchBuf(needed)
	for i, offset := range tw.offsets {
		binary.LittleEndian.PutUint32(buffer[i*4:i*4+4], offset)
	}
	return buffer
}

func (tw *TempWriter) buildMetaBlock() error {
	return nil
}

func (tw *TempWriter) scratchBuf(needed int) []byte {
	available := tw.buf.Available()
	if needed > available {
		tw.buf.Grow(needed)
	}
	buf := tw.buf.AvailableBuffer()
	return buf[:needed]
}

func (tw *TempWriter) trackOffset(n uint32) {
	tw.offsets = append(tw.offsets, tw.nextOffset)
	tw.nextOffset += n
}

//
//func (tw *TempWriter) Write() error {
//	_, err := tw.file.Write(tw.buf.Bytes())
//	if err != nil {
//		return err
//	}
//	return nil
//}
