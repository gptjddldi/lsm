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
	return nil
}
