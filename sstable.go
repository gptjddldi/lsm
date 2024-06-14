package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/gptjddldi/lsm/db/encoder"
	"io"
	"os"
)

type SSTable struct {
	index *Index
	// todo: bloom filter
	file *os.File
}

type SSTableIterator struct {
	sstable    *SSTable
	reader     *bufio.Reader
	entry      *DataEntry
	curOffset  int
	stopOffset int
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
	return sst
}

func (s *SSTable) readFooter() ([]byte, error) {
	fileSize, err := s.file.Stat()
	if err != nil {
		return nil, err
	}
	footerSize := 8
	footer := make([]byte, footerSize)
	_, err = s.file.ReadAt(footer, fileSize.Size()-int64(footerSize))
	if err != nil {
		return nil, err
	}
	return footer, nil
}

func (s *SSTable) indexLength() (int, error) {
	footer, err := s.readFooter()
	if err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint32(footer[:4]) - 8), nil
}

func (s *SSTable) indexOffset() (int, error) {
	indexLength, err := s.indexLength()
	if err != nil {
		return 0, err
	}
	fileSize, err := s.file.Stat()
	if err != nil {
		return 0, err
	}
	return int(fileSize.Size()) - indexLength - 8, nil
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

func (s *SSTable) Get(searchKey []byte) (*encoder.EncodedValue, error) {
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
	// Seek to the offset
	_, err := s.file.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Read the block
	block := make([]byte, length)
	_, err = s.file.Read(block)
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

func (s *SSTable) PUT(key, val []byte) error {
	return nil
}

func (s *SSTable) Iterator() *SSTableIterator {
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
	startPosition := it.curOffset

	keyLen, err := binary.ReadUvarint(it.reader)
	if err != nil {
		return false, err
	}
	it.curOffset += binary.PutUvarint(make([]byte, 10), keyLen)

	valLen, err := binary.ReadUvarint(it.reader)
	if err != nil {
		return false, err
	}
	it.curOffset += binary.PutUvarint(make([]byte, 10), valLen)

	if it.curOffset+int(keyLen+valLen) > it.stopOffset {
		it.curOffset = startPosition
		return false, nil
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(it.reader, key); err != nil {
		return false, err
	}
	it.curOffset += int(keyLen)

	value := make([]byte, valLen)
	if _, err := io.ReadFull(it.reader, value); err != nil {
		return false, err
	}
	it.curOffset += int(valLen)
	it.entry = &DataEntry{key: key, value: value}

	return true, nil
}

func (it *SSTableIterator) Close() error {
	return it.sstable.file.Close()
}

func (it *SSTableIterator) Key() []byte {
	return it.entry.key
}

func (it *SSTableIterator) Value() []byte {
	return it.entry.value[1:]
}

func (it *SSTableIterator) OpType() encoder.OpType {
	return encoder.OpType(it.entry.value[0])
}
