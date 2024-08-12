package lsm

import (
	"github.com/bits-and-blooms/bloom/v3"
	"sync"
)

type BloomFilter struct {
	mutex  *sync.RWMutex
	filter *bloom.BloomFilter
}

func NewBloomFilter() *BloomFilter {
	return &BloomFilter{
		mutex:  &sync.RWMutex{},
		filter: bloom.NewWithEstimates(1000000, 0.01), // 1% false positive rate
	}
}

func LoadBloomFilter(data []byte) (*BloomFilter, error) {
	bf := NewBloomFilter()
	err := bf.Load(data)
	if err != nil {
		return nil, err
	}
	return bf, nil
}

func (bf *BloomFilter) Add(data []byte) {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()

	bf.filter.Add(data)
}

func (bf *BloomFilter) Contains(data []byte) bool {
	bf.mutex.RLock()
	defer bf.mutex.RUnlock()

	return bf.filter.Test(data)
}

func (bf *BloomFilter) Encode() ([]byte, error) {
	bf.mutex.RLock()
	defer bf.mutex.RUnlock()

	return bf.filter.GobEncode()
}

func (bf *BloomFilter) Load(data []byte) error {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()

	return bf.filter.GobDecode(data)
}
