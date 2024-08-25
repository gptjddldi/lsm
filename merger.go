package lsm

import (
	"bytes"
	"container/heap"
	"github.com/gptjddldi/lsm/db/encoder"
)

type MinHeapItem struct {
	iterator  *SSTableIterator
	key       []byte
	value     []byte
	opType    encoder.OpType
	Timestamp int64 // todo: implement time stamp
}

type MinHeap []*MinHeapItem

func (h MinHeap) Len() int { return len(h) }

func (h MinHeap) Less(i, j int) bool { return bytes.Compare(h[i].key, h[j].key) < 0 }

func (h MinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(*MinHeapItem))
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func (db *DB) mergeIterators(iterators []*SSTableIterator, targetLevel int) ([]*SSTable, error) {
	minHeap := &MinHeap{}
	heap.Init(minHeap)

	for _, it := range iterators {
		_, err := it.Next()
		if err != nil {
			return nil, err
		}

		heap.Push(minHeap, &MinHeapItem{
			iterator: it,
			key:      it.Key(),
			value:    it.Value(),
			opType:   it.OpType(),
			//Timestamp: int64(idx), // todo: temporary
		})
	}
	de := make([]*DataEntry, 0)
	totalSize := 0
	sstables := make([]*SSTable, 0)

	var before []byte
	var nextIter func(iterator *SSTableIterator) error
	nextIter = func(iterator *SSTableIterator) error {
		ok, err := iterator.Next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		heap.Push(minHeap, &MinHeapItem{
			iterator: iterator,
			key:      iterator.Key(),
			opType:   iterator.OpType(),
			value:    iterator.Value(),
		})
		return nil
	}

	for minHeap.Len() > 0 {
		item := heap.Pop(minHeap).(*MinHeapItem)

		if bytes.Compare(before, item.key) == 0 {
			err := nextIter(item.iterator)
			if err != nil {
				return nil, err
			}
			continue
		}

		if item.opType != encoder.OpTypeDelete {
			de = append(de, &DataEntry{
				key:    item.key,
				value:  item.value,
				opType: item.opType,
			})
			before = item.key
			totalSize += len(item.key) + len(item.value) + 1
		}

		if totalSize >= calculateMaxFileSize(targetLevel) {
			iter, err := db.writeIterator(de, targetLevel)
			if err != nil {
				return nil, err
			}
			sstables = append(sstables, iter)
			de = make([]*DataEntry, 0)
			totalSize = 0
		}

		err := nextIter(item.iterator)
		if err != nil {
			return nil, err
		}
	}

	iter, err := db.writeIterator(de, targetLevel)
	if err != nil {
		return nil, err
	}

	sstables = append(sstables, iter)

	return sstables, nil
}

// level 마다 개당 용량있고, 그거에 도달하면 호출됨
func (db *DB) writeIterator(entries []*DataEntry, targetLevel int) (*SSTable, error) {
	meta := db.dataStorage.PrepareNewFile(targetLevel)
	f, err := db.dataStorage.OpenFileForWriting(meta)
	if err != nil {
		return nil, err
	}

	writer := NewTempWriter(f)
	writer.Write(entries)

	sst, err := db.OpenSSTable(f)
	if err != nil {
		return nil, err
	}
	return sst, nil
}
