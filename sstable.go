package lsm

import (
	"encoding/binary"
	"io"
	"os"
)

type SSTable struct {
	//index []byte
	// todo: bloom filter, index
	file *os.File
}
type SSTableIterator struct {
	sstable    *SSTable
	reader     *Reader
	entry      *DataEntry
	curOffset  int
	stopOffset int
}

func NewSSTable(file *os.File) *SSTable {
	return &SSTable{
		file: file,
	}
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

func (s *SSTable) indexOffset() (int, error) {
	footer, err := s.readFooter()
	if err != nil {
		return 0, err
	}
	indexLength := binary.LittleEndian.Uint32(footer[:4])

	fileSize, err := s.file.Stat()
	if err != nil {
		return 0, err
	}
	return int(fileSize.Size()) - int(indexLength) - 8, nil
}

func (s *SSTable) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (s *SSTable) PUT(key, val []byte) error {
	return nil
}

func (s *SSTable) Iterator() *SSTableIterator {
	reader, err := NewReader(s.file)
	if err != nil {
		return nil
	}
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

	keyLen, err := binary.ReadUvarint(it.reader.br)
	if err != nil {
		return false, err
	}
	it.curOffset += binary.PutUvarint(make([]byte, 10), keyLen)

	valLen, err := binary.ReadUvarint(it.reader.br)
	if err != nil {
		return false, err
	}
	it.curOffset += binary.PutUvarint(make([]byte, 10), valLen)

	if it.curOffset+int(keyLen+valLen) > it.stopOffset {
		it.curOffset = startPosition
		return false, nil
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(it.reader.br, key); err != nil {
		return false, err
	}
	it.curOffset += int(keyLen)

	value := make([]byte, valLen)
	if _, err := io.ReadFull(it.reader.br, value); err != nil {
		return false, err
	}
	it.curOffset += int(valLen)
	it.entry = &DataEntry{key: key, value: value}

	return true, nil
}
