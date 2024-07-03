package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"

	"github.com/gptjddldi/lsm/db/encoder"
)

type SSTable struct {
	index       *Index
	bloomFilter *BloomFilter

	file *os.File

	minKey []byte
	maxKey []byte
}

type SSTableIterator struct {
	sstable    *SSTable
	reader     *bufio.Reader
	entry      *DataEntry
	curOffset  uint64
	stopOffset uint64
}

func NewSSTable(file *os.File) *SSTable {
	sst := &SSTable{
		file: file,
	}
	index, err := sst.readIndex()
	if err != nil {
		return nil
	}
	sst.index = index

	bloomFilter, err := sst.readBloomFilter()
	if err != nil {
		return nil
	}
	sst.bloomFilter = bloomFilter

	sst.minKey = sst.getFirstKeyFromFile()
	sst.maxKey = index.entries[len(index.entries)-1].key

	return sst
}

func (s *SSTable) getFirstKeyFromFile() []byte {
	firstIndexEntry := s.index.entries[0]
	length := binary.LittleEndian.Uint32(firstIndexEntry.value[4:8])

	buf, err := s.readBlockAt(0, length)
	if err != nil {
		return nil
	}

	var keyLen uint64
	var n, offset int
	keyLen, n = binary.Uvarint(buf[offset:])

	offset += n
	_, n = binary.Uvarint(buf[offset:])
	offset += n
	key := buf[offset : offset+int(keyLen)]
	offset += int(keyLen)

	return key
}

func (s *SSTable) readFooter() ([]byte, error) {
	fileSize, err := s.file.Stat()
	if err != nil {
		return nil, err
	}
	footerSize := 16
	footer := make([]byte, footerSize)
	_, err = s.file.ReadAt(footer, fileSize.Size()-int64(footerSize))
	if err != nil {
		return nil, err
	}
	return footer, nil
}

func (s *SSTable) indexLength() (uint64, error) {
	footer, err := s.readFooter()
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(footer[:8]), nil
}

func (s *SSTable) indexOffset() (uint64, error) {
	footer, err := s.readFooter()
	if err != nil {
		return 0, err
	}
	fileSize, err := s.file.Stat()
	if err != nil {
		return 0, err
	}

	indexSize := binary.LittleEndian.Uint64(footer[:8])
	bloomFilterSize := binary.LittleEndian.Uint64(footer[8:])

	totalSize := uint64(fileSize.Size())

	// indexOffset 계산
	// 전체 크기 - (footer 크기 + bloomFilter 크기 + index 크기)
	indexOffset := totalSize - (16 + bloomFilterSize + indexSize)

	return indexOffset, nil
}

func (s *SSTable) readIndex() (*Index, error) {
	indexLength, err := s.indexLength()
	if err != nil {
		return nil, err
	}
	indexOffset, err := s.indexOffset()
	if err != nil {
		return nil, err
	}
	index := make([]byte, indexLength)
	_, err = s.file.ReadAt(index, int64(indexOffset))
	if err != nil {
		return nil, err
	}
	return NewIndex(index), nil
}

func (s *SSTable) bloomFilterOffset() (uint64, error) {
	fileSize, err := s.file.Stat()
	if err != nil {
		return 0, err
	}

	bloomFilterSize, err := s.bloomFilterSize()
	if err != nil {
		return 0, err
	}

	// 전체 파일 크기
	totalSize := uint64(fileSize.Size())

	// bloomFilterOffset 계산
	// 전체 크기 - (footer 크기 + bloomFilter 크기)
	bloomFilterOffset := totalSize - (16 + bloomFilterSize)

	return bloomFilterOffset, nil
}

func (s *SSTable) bloomFilterSize() (uint64, error) {
	footer, err := s.readFooter()
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(footer[8:]), nil
}

func (s *SSTable) readBloomFilter() (*BloomFilter, error) {
	bloomFilterOffset, err := s.bloomFilterOffset()
	if err != nil {
		return nil, err
	}
	bloomFilterSize, err := s.bloomFilterSize()
	if err != nil {
		return nil, err
	}
	bloomFilter := make([]byte, bloomFilterSize)
	_, err = s.file.ReadAt(bloomFilter, int64(bloomFilterOffset))
	if err != nil {
		return nil, err
	}

	return LoadBloomFilter(bloomFilter)
}

func (s *SSTable) Contains(searchKey []byte) bool {
	return s.bloomFilter.Contains(searchKey)
}

func (s *SSTable) Get(searchKey []byte) (*encoder.EncodedValue, error) {
	if !s.Contains(searchKey) {
		return nil, ErrorKeyNotFound
	}

	ie := s.index.Get(searchKey)

	offset := binary.LittleEndian.Uint32(ie.value[:4])
	length := binary.LittleEndian.Uint32(ie.value[4:8])

	block, err := s.readBlockAt(offset, length)
	if err != nil {
		return nil, err
	}

	value, err := s.sequentialSearchBuf(block, searchKey)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (s *SSTable) readBlockAt(offset, length uint32) ([]byte, error) {
	// Create a byte slice to hold the block
	block := make([]byte, length)

	// Read the block at the specified offset without changing the file's current position
	_, err := s.file.ReadAt(block, int64(offset))
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (s *SSTable) sequentialSearchBuf(buf []byte, searchKey []byte) (*encoder.EncodedValue, error) {
	var offset int
	for {
		var keyLen, valLen uint64
		var n int
		keyLen, n = binary.Uvarint(buf[offset:])
		if n <= 0 {
			break
		}
		offset += n
		valLen, n = binary.Uvarint(buf[offset:])
		offset += n
		key := buf[offset : offset+int(keyLen)]
		offset += int(keyLen)
		val := buf[offset : offset+int(valLen)]
		offset += int(valLen)
		cmp := bytes.Compare(searchKey, key)
		if cmp == 0 {
			return encoder.Decode(val), nil
		}
		if cmp < 0 {
			break
		}
	}
	return nil, ErrorKeyNotFound
}

func (s *SSTable) IsInKeyRange(min, max []byte) bool {
	if bytes.Compare(s.minKey, max) > 0 {
		return false
	}
	if bytes.Compare(s.maxKey, min) < 0 {
		return false
	}
	return true
}

func (s *SSTable) PUT(key, val []byte) error {
	return nil
}

func (s *SSTable) Iterator() *SSTableIterator {
	s.file.Seek(0, 0) // todo: 언제 file 의 위치가 변경되는지 확인 필요함
	reader := bufio.NewReader(s.file)
	indexOffset, err := s.indexOffset()
	if err != nil {
		return nil
	}
	return &SSTableIterator{
		sstable:    s,
		reader:     reader,
		entry:      &DataEntry{},
		stopOffset: indexOffset,
	}
}

func (it *SSTableIterator) Next() (bool, error) {
	if it.curOffset >= it.stopOffset {
		return false, nil
	}
	entry, offset := readEntry(it.reader)
	it.entry = entry
	it.curOffset += offset

	return true, nil
}

func (it *SSTableIterator) Close() error {
	return it.sstable.file.Close()
}

func (it *SSTableIterator) Key() []byte {
	return it.entry.key
}

func (it *SSTableIterator) Value() []byte {
	return it.entry.value
}

func (it *SSTableIterator) OpType() encoder.OpType {
	return it.entry.opType
}
