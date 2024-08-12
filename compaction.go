package lsm

import (
	"fmt"
)

func (db *DB) compactLevel(level int) error {
	if level == 0 {
		return db.compactLevel0()
	} else {
		return db.compactLevelN(level)
	}
}

func (db *DB) compactLevel0() error {
	iterators0, err := db.getIteratorsForLevel(0)
	if err != nil {
		return err
	}

	iterators1, err := db.getIteratorsForLevel(1)
	if err != nil {
		return err
	}

	iterators := append(iterators0, iterators1...)
	sstList, err := db.mergeIterators(iterators, 1)
	if err != nil {
		return err
	}

	db.deleteSStableAtLevel(0, iterators0)
	db.deleteSStableAtLevel(1, iterators1)
	db.levels[1].sstables = append(db.levels[1].sstables, sstList...)

	db.compactionChan <- 1
	fmt.Println("Compaction END")

	return nil
}

func (db *DB) getIteratorsForLevel(level int) ([]*SSTableIterator, error) {
	iterators := make([]*SSTableIterator, 0, len(db.levels[level].sstables))
	for _, sstable := range db.levels[level].sstables {
		iter, err := sstable.Iterator()
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, iter)
	}
	return iterators, nil
}

func (db *DB) compactLevelN(level int) error {
	fmt.Printf("level: %d, Compaction Start\n", level)

	targetSst := db.LeastSstableAtLevel(level)
	minKey, maxKey := targetSst.minKey, targetSst.maxKey

	iterators, err := db.getCompactionIterators(level, targetSst, minKey, maxKey)
	if err != nil {
		return err
	}

	sstList, err := db.mergeIterators(iterators, level+1)
	if err != nil {
		return err
	}

	db.deleteSStableAtLevel(level, iterators[:1])   // Delete from current level
	db.deleteSStableAtLevel(level+1, iterators[1:]) // Delete from next level

	db.levels[level+1].sstables = append(db.levels[level+1].sstables, sstList...)

	db.compactionChan <- level + 1
	fmt.Printf("level: %d, Compaction END\n", level)

	return nil
}

func (db *DB) getCompactionIterators(level int, targetSst *SSTable, minKey, maxKey []byte) ([]*SSTableIterator, error) {
	iter, err := targetSst.Iterator()
	if err != nil {
		return nil, err
	}

	involvedIter, err := db.involvedIterators(level+1, minKey, maxKey)
	if err != nil {
		return nil, err
	}

	return append([]*SSTableIterator{iter}, involvedIter...), nil
}

func (db *DB) needLevelNCompaction(level int) bool {
	db.compactionMu.RLock()
	defer db.compactionMu.RUnlock()

	if db.isCompacting[level] {
		return false
	}

	if level == 0 {
		return len(db.levels[0].sstables) >= l0Capacity
	}

	return db.levels[level].TotalSize() > calculateLevelSize(level)
}
func (db *DB) involvedIterators(level int, minKey, maxKey []byte) ([]*SSTableIterator, error) {
	iterators := make([]*SSTableIterator, 0)
	for _, sstable := range db.levels[level].sstables {
		if sstable.IsInKeyRange(minKey, maxKey) {
			iter, err := sstable.Iterator()
			if err != nil {
				return nil, err
			}
			iterators = append(iterators, iter)
		}
	}
	return iterators, nil
}
