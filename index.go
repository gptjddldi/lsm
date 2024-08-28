package lsm

import (
	"bytes"
	"encoding/binary"
)

type BaseIndex interface {
	Get(searchKey []byte) IndexEntry
	FirstEntry() IndexEntry
	LastEntry() IndexEntry
}

type Index struct {
	entries []IndexEntry
}

type IndexEntry struct {
	key   []byte
	value []byte
}

func NewIndex(indexBytes []byte) BaseIndex {
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
	offset := idx.binarySearch(searchKey, low, high)

	return idx.entries[offset]
}

func (idx *Index) FirstEntry() IndexEntry {
	return idx.entries[0]
}

func (idx *Index) LastEntry() IndexEntry {
	return idx.entries[len(idx.entries)-1]
}

func (idx *Index) binarySearch(searchKey []byte, low, high int) int {
	high = min(high, len(idx.entries))
	for low < high {
		mid := (low + high) / 2
		cmp := bytes.Compare(searchKey, idx.entries[mid].key)
		if cmp > 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}
