package lsm

import (
	"bytes"
	"container/heap"
	"github.com/gptjddldi/lsm/db/encoder"
	"os"
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

func mergeFiles(iterators []*SSTableIterator, file *os.File) error {
	writer := NewTempWriter(file)
	defer file.Close()

	minHeap := &MinHeap{}
	heap.Init(minHeap)

	for idx, it := range iterators {
		_, err := it.Next()
		if err != nil {
			return err
		}

		if it.OpType() == encoder.OpTypeDelete {
			it.Next()
			continue
		}
		heap.Push(minHeap, &MinHeapItem{
			iterator:  it,
			key:       it.Key(),
			value:     it.Value(),
			opType:    it.OpType(),
			Timestamp: int64(idx), // todo: temporary
		})
	}
	var de []*DataEntry

	for minHeap.Len() > 0 {
		item := heap.Pop(minHeap).(*MinHeapItem)

		if item.opType != encoder.OpTypeDelete {
			de = append(de, &DataEntry{
				key:   item.key,
				value: item.value,
			})
		}

		ok, err := item.iterator.Next()
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if item.iterator.OpType() == encoder.OpTypeDelete {
			item.iterator.Next()
		}
		heap.Push(minHeap, &MinHeapItem{
			iterator: item.iterator,
			key:      item.iterator.Key(),
			value:    item.iterator.Value(),
			opType:   item.iterator.OpType(),
		})
	}

	return writer.Write(de)
}

//
//
//package lsm
//
//import (
//	"container/heap"
//	"github.com/gptjddldi/lsm/db/encoder"
//	"github.com/gptjddldi/lsm/db/skiplist"
//)
//
//type MergeHeapItem struct {
//	iterator *SSTableIterator
//	key      []byte
//	value    []byte
//	opType   encoder.OpType
//	//index    int
//
//	Timestamp int64
//}
//
//type MergeHeap []*MergeHeapItem
//
//func (h MergeHeap) Len() int {
//	return len(h)
//}
//
//func (h MergeHeap) Less(i, j int) bool {
//	return h[i].Timestamp < h[j].Timestamp
//}
//
//func (h MergeHeap) Swap(i, j int) {
//	h[i], h[j] = h[j], h[i]
//}
//
//func (h *MergeHeap) Push(x interface{}) {
//	*h = append(*h, x.(*MergeHeapItem))
//}
//
//func (h *MergeHeap) Pop() interface{} {
//	old := *h
//	n := len(old)
//	x := old[n-1]
//	*h = old[0 : n-1]
//	return x
//}
//
//// Performs a k-way merge on SSTable iterators of possibly overlapping ranges
//// and merges them into a single range without any duplicate entries.
//// Deduplication is done by keeping track of the most recent entry for each key
//// and discarding the older ones using the timestamp.
//func mergeIterators(iterators []*SSTableIterator) []*DataEntry {
//	mergeHeap := &MergeHeap{}
//	heap.Init(mergeHeap)
//
//	var results []*DataEntry
//
//	// Keep track of the most recent entry for each key, in sorted order of keys.
//	seen := skiplist.NewSkipList()
//
//	// Add the iterators to the heap.
//	for _, it := range iterators {
//		if it == nil {
//			continue
//		}
//		heap.Push(mergeHeap, MergeHeapItem{
//			iterator: it,
//			key:      it.Key(),
//			value:    it.Value(),
//			opType:   it.OpType(),
//		})
//	}
//
//	for mergeHeap.Len() > 0 {
//		// Pop the min entry from the heap.
//		minEntry := heap.Pop(mergeHeap).(MergeHeapItem)
//		previousValue, err := seen.Find(minEntry.key)
//
//		// Check if this key has been seen before.
//		if previousValue != nil {
//			// If the previous entry has a smaller timestamp, then we need to
//			// replace it with the more recent entry.
//			if previousValue.val.(MergeHeapItem).Timestamp < minEntry.entry.Timestamp {
//				seen.Set(minEntry.entry.Key, minEntry)
//			}
//		} else {
//			// Add the entry to the seen list.
//			seen.Set(minEntry.entry.Key, minEntry)
//		}
//
//		// Add the next element from the same list to the heap
//		if minEntry.iterator.Next() != nil {
//			nextEntry := minEntry.iterator.Value
//			heap.Push(mergeHeap, MergeHeapItem{entry: nextEntry, Iterator: minEntry.iterator})
//		}
//	}
//
//	// Iterate through the seen list and add the values to the results.
//	iter := seen.Iterator()
//	for iter != nil {
//		entry := iter.Value.(MergeHeapItem)
//		if entry.entry.Command == Command_DELETE {
//			iter = iter.Next()
//			continue
//		}
//		results = append(results, entry.entry)
//		iter = iter.Next()
//	}
//
//	return results
//}
