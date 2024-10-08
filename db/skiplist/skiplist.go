package skiplist

import (
	"bytes"
	"errors"
	"github.com/gptjddldi/lsm/db/compare"
	"math"
	"math/rand"
)

const (
	MaxHeight = 16
	PValue    = 0.5
)

var probabilities [MaxHeight]uint32

type node struct {
	key   []byte
	val   []byte
	tower [MaxHeight]*node
}

func init() {
	probability := 1.0

	for level := 0; level < MaxHeight; level++ {
		probabilities[level] = uint32(probability * float64(math.MaxUint32))
		probability *= PValue
	}
}

func randomHeight() int {
	seed := rand.Uint32()

	height := 1
	for height < MaxHeight && seed <= probabilities[height] {
		height++
	}
	return height
}

type SkipList struct {
	head            *node
	height          int
	useLearnedIndex bool
}

func NewSkipList(useLearnedIndex bool) (sl *SkipList) {
	sl = &SkipList{useLearnedIndex: useLearnedIndex}
	sl.head = &node{}
	sl.height = 1
	return
}

func (sl *SkipList) search(key []byte) (*node, [MaxHeight]*node) {
	var next *node
	var journey [MaxHeight]*node

	prev := sl.head
	for level := sl.height - 1; level >= 0; level-- {
		for next = prev.tower[level]; next != nil; next = prev.tower[level] {
			if compare.Compare(next.key, key, sl.useLearnedIndex) >= 0 {
				break
			}
			prev = next
		}
		journey[level] = prev
	}
	if next != nil && bytes.Equal(next.key, key) {
		return next, journey
	}
	return nil, journey
}

func (sl *SkipList) Find(key []byte) ([]byte, error) {
	found, _ := sl.search(key)
	if found == nil {
		return nil, errors.New("key not found")
	}
	return found.val, nil
}

func (sl *SkipList) Insert(key, val []byte) {
	found, journey := sl.search(key)
	if found != nil {
		found.val = val
		return
	}
	height := randomHeight()
	nd := &node{key: key, val: val}

	for level := 0; level < height; level++ {
		prev := journey[level]

		if prev == nil {
			prev = sl.head
		}
		nd.tower[level] = prev.tower[level]
		prev.tower[level] = nd
	}
	if height > sl.height {
		sl.height = height
	}
}

func (sl *SkipList) Delete(key []byte) bool {
	found, journey := sl.search(key)
	if found == nil {
		return false
	}
	for level := 0; level < sl.height; level++ {
		if journey[level].tower[level] != found {
			break
		}
		journey[level].tower[level] = found.tower[level]
		found.tower[level] = nil
	}
	sl.Shrink()
	found = nil
	return true
}

func (sl *SkipList) Shrink() {
	for level := sl.height - 1; level >= 0; level-- {
		if sl.head.tower[level] == nil {
			sl.height--
		}
	}
}

func (sl *SkipList) String() string {
	v := &visualizer{sl}
	return v.visualize()
}
