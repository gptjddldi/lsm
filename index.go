package lsm

import (
	"bytes"
	"encoding/binary"
)

type Index struct {
	entries []IndexEntry
}

type IndexEntry struct {
	key   []byte
	value []byte
}

func NewIndex(indexBytes []byte) *Index {
	var entries []IndexEntry
	buf := bytes.NewBuffer(indexBytes)

	for {
		keyLen, _ := binary.ReadUvarint(buf)
		if keyLen == 0 {
			break
		}
		valLen, _ := binary.ReadUvarint(buf)
		if valLen == 0 {
			break
		}
		valLen -= 1 // subtract the added 1

		key := make([]byte, keyLen)
		buf.Read(key)

		// Skip the operation type byte
		buf.ReadByte()

		val := make([]byte, valLen)
		buf.Read(val)

		entries = append(entries, IndexEntry{key: key, value: val})
	}
	return &Index{entries: entries}
}

func (idx *Index) Get(searchKey []byte) IndexEntry {
	low, high := 0, len(idx.entries)-1
	var mid int
	for low < high {
		mid = (low + high) / 2
		cmp := bytes.Compare(searchKey, idx.entries[mid].key)
		if cmp > 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return idx.entries[low]
}
