package lsm

func (db *DB) compactLevel(level int) error {
	if level == 0 {
		return db.compactLevel0()
	}
	return db.compactLevelN(level)
}

func (db *DB) compactLevel0() error {
	return nil
}

func (db *DB) compactLevelN(level int) error {
	return nil
}
