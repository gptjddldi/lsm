package lsm

func (db *DB) compactLevel(level int) error {
	if level == 0 && db.needL0Compaction() {
		return db.compactLevel0()
	} else if level > 0 && db.needLevelNCompaction(level) {
		return db.compactLevelN(level)
	}
	return nil
}

func (db *DB) compactLevel0() error {
	iterators0 := make([]*SSTableIterator, 0)
	for _, sstable := range db.levels[0].sstables {
		iterators0 = append(iterators0, sstable.Iterator())
	}
	iterators1 := make([]*SSTableIterator, 0)
	for _, sstable := range db.levels[1].sstables {
		iterators1 = append(iterators1, sstable.Iterator())
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

	return nil
}

func (db *DB) compactLevelN(level int) error {
	iterators := make([]*SSTableIterator, 0)
	targetSst := db.LeastSstableAtLevel(level)
	minKey := targetSst.minKey
	maxKey := targetSst.maxKey
	iterators = append(iterators, targetSst.Iterator())
	iterators = append(iterators, db.involvedIterators(level+1, minKey, maxKey)...)

	sstList, err := db.mergeIterators(iterators, level+1)
	if err != nil {
		return err
	}
	db.deleteSStableAtLevel(level, iterators)
	db.deleteSStableAtLevel(level+1, iterators)

	db.levels[level+1].sstables = append(db.levels[level+1].sstables, sstList...)

	db.compactionChan <- level + 1
	return nil
}
