package skiplist

type Iterator struct {
	current *node
}

func (sl *SkipList) Iterator() *Iterator {
	return &Iterator{sl.head.tower[0]}
}

func (it *Iterator) Current() ([]byte, []byte) {
	return it.current.key, it.current.val
}

func (it *Iterator) HasNext() bool {
	return it.current.tower[0] != nil
}

func (it *Iterator) Next() ([]byte, []byte) {
	it.current = it.current.tower[0]
	if it.current == nil {
		return nil, nil
	}
	return it.current.key, it.current.val
}
