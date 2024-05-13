package sstable

import (
	"bytes"
	"encoding/binary"
)

type blockReader struct {
	buf        []byte
	offsets    []byte
	numOffsets int
}

func (br *blockReader) readOffsetAt(pos int) int {
	offset, _, _ := br.fetchDataFor(pos)
	return offset
}

func (br *blockReader) readKeyAt(pos int) []byte {
	_, key, _ := br.fetchDataFor(pos)
	return key
}

func (br *blockReader) readValAt(pos int) []byte {
	_, _, val := br.fetchDataFor(pos)
	return val
}

func (br *blockReader) fetchDataFor(position int) (kvOffset int, key, val []byte) {
	var keyLen, valLen uint64
	var n int
	kvOffset = int(binary.LittleEndian.Uint32(br.offsets[position*4 : position*4+4]))
	offset := kvOffset
	keyLen, n = binary.Uvarint(br.buf[offset:])
	offset += n
	valLen, n = binary.Uvarint(br.buf[offset:])
	offset += n
	key = br.buf[offset : offset+int(keyLen)]
	offset += int(keyLen)
	val = br.buf[offset : offset+int(valLen)]
	return
}

func (br *blockReader) search(searchKey []byte) int {
	low, high := 0, br.numOffsets
	var mid int
	for low < high {
		mid = (low + high) / 2
		key := br.readKeyAt(mid)
		cmp := bytes.Compare(searchKey, key)
		if cmp > 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}
