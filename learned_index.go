package lsm

import (
	"bytes"
	"encoding/binary"
	"github.com/gptjddldi/lsm/db/compare"
	"github.com/gptjddldi/lsm/db/regression"
)

type LearnedIndex struct {
	entries []IndexEntry
	learned *regression.LinearRegression
}

func NewLearnedIndex(indexBytes []byte) BaseIndex {
	var entries []IndexEntry
	buf := bytes.NewBuffer(indexBytes)
	x := make([]uint64, 0)
	y := make([]uint64, 0)
	i := 0
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

		str := uint64(compare.ByteToInt(key))
		x = append(x, str)
		y = append(y, uint64(i))
		i++

		entries = append(entries, IndexEntry{key: key, value: val})
	}
	b := regression.NewRegression()
	b.Train(x, y)

	return &LearnedIndex{entries: entries, learned: b}
}

func (idx *LearnedIndex) Get(searchKey []byte) IndexEntry {
	key := uint64(compare.ByteToInt(searchKey))
	predicted := idx.learned.Predict(key)
	low := int(predicted) - int(float64(len(idx.entries))*0.001)  // -1%
	high := int(predicted) + int(float64(len(idx.entries))*0.001) // +1%

	offset := idx.binarySearch(searchKey, low, high)

	return idx.entries[offset]
}

func (idx *LearnedIndex) FirstEntry() IndexEntry {
	return idx.entries[0]
}

func (idx *LearnedIndex) LastEntry() IndexEntry {
	return idx.entries[len(idx.entries)-1]
}

func (idx *LearnedIndex) binarySearch(searchKey []byte, low, high int) int {
	high = min(high, len(idx.entries)-1)
	low = min(max(low, 0), high)
	for low < high {
		mid := (low + high) / 2
		cmp := compare.Compare(searchKey, idx.entries[mid].key, true)
		if cmp > 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}
