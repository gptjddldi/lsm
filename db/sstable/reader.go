package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"lsm/db/encoder"
)

var ErrorKeyNotFound = errors.New("key not found")

const (
	footerSizeInBytes = 8
)

type Reader struct {
	file     statReaderAtCloser
	br       *bufio.Reader
	buf      []byte
	encoder  *encoder.Encoder
	fileSize int64
}

type statReaderAtCloser interface {
	Stat() (fs.FileInfo, error)
	io.ReaderAt
	io.Closer
}

func NewReader(file io.Reader) (*Reader, error) {
	r := &Reader{}
	r.file, _ = file.(statReaderAtCloser)
	r.br = bufio.NewReader(file)
	r.buf = make([]byte, 0, maxBlockSize)
	err := r.initFileSize()
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Reader) Get(searchKey []byte) (*encoder.EncodedValue, error) {
	//return r.sequentialSearch(searchKey)
	return r.binarySearch(searchKey)
}

func (r *Reader) sequentialSearch(searchKey []byte) (*encoder.EncodedValue, error) {
	for {
		keyLen, err := binary.ReadUvarint(r.br)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		valLen, err := binary.ReadUvarint(r.br)
		if err != nil {
			return nil, err
		}
		needed := int(keyLen + valLen)
		if cap(r.buf) < needed {
			r.buf = make([]byte, needed)
		}
		buf := r.buf[:needed]
		_, err = io.ReadFull(r.br, buf)
		if err != nil {
			return nil, err
		}
		key := buf[:keyLen]
		val := buf[keyLen:]

		if bytes.Compare(key, searchKey) == 0 {
			return r.encoder.Decode(val), nil
		}
	}
	return nil, ErrorKeyNotFound
}

func (r *Reader) binarySearch(searchKey []byte) (*encoder.EncodedValue, error) {
	footer, err := r.readFooter()
	if err != nil {
		return nil, err
	}

	index, err := r.readIndexBlock(footer)
	if err != nil {
		return nil, err
	}

	pos := index.search(searchKey)
	indexEntry := index.readValAt(pos)

	data, err := r.readDataBlock(indexEntry)
	if err != nil {
		return nil, err
	}

	return r.sequentialSearchBuf(data, searchKey)
}

func (r *Reader) Close() error {
	err := r.file.Close()
	if err != nil {
		return err
	}
	r.file = nil
	r.br = nil
	return nil
}

func (r *Reader) initFileSize() error {
	info, err := r.file.Stat()
	if err != nil {
		return err
	}
	r.fileSize = info.Size()
	return nil
}

func (r *Reader) readFooter() ([]byte, error) {
	buf := r.buf[:footerSizeInBytes]
	footerOffset := r.fileSize - footerSizeInBytes
	_, err := r.file.ReadAt(buf, footerOffset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (r *Reader) prepareBlockReader(buf, footer []byte) *blockReader {
	indexLength := int(binary.LittleEndian.Uint32(footer[:4]))
	numOffsets := int(binary.LittleEndian.Uint32(footer[4:]))
	buf = buf[:indexLength]
	return &blockReader{
		buf:        buf,
		offsets:    buf[indexLength-(numOffsets+2)*4:],
		numOffsets: numOffsets,
	}
}

func (r *Reader) readIndexBlock(footer []byte) (*blockReader, error) {
	b := r.prepareBlockReader(r.buf, footer)
	indexOffset := r.fileSize - int64(len(b.buf))
	_, err := r.file.ReadAt(b.buf, indexOffset)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (r *Reader) readDataBlock(indexEntry []byte) ([]byte, error) {
	var err error
	val := r.encoder.Decode(indexEntry).Value()
	offset := binary.LittleEndian.Uint32(val[:4])
	length := binary.LittleEndian.Uint32(val[4:])
	buf := r.buf[:length]
	_, err = r.file.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (r *Reader) sequentialSearchBuf(buf []byte, searchKey []byte) (*encoder.EncodedValue, error) {
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
		key := buf[:keyLen]
		copy(key[:], buf[offset:offset+int(valLen)])
		offset += int(keyLen)
		val := buf[offset : offset+int(valLen)]
		offset += int(valLen)
		cmp := bytes.Compare(searchKey, key)
		if cmp == 0 {
			return r.encoder.Decode(val), nil
		}
		if cmp < 0 {
			break
		}
	}
	return nil, ErrorKeyNotFound
}
