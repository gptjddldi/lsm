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
	footerSizeInBytes = 4
)

type Reader struct {
	file    statCloser
	br      *bufio.Reader
	buf     []byte
	encoder *encoder.Encoder
}

type statCloser interface {
	Stat() (fs.FileInfo, error)
	io.Closer
}

func NewReader(file io.Reader) *Reader {
	r := &Reader{}
	r.file, _ = file.(statCloser)
	r.br = bufio.NewReader(file)
	r.buf = make([]byte, 0, 1024)
	return r
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
	info, err := r.file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()

	buf := r.buf[:fileSize]
	_, err = io.ReadFull(r.br, buf)
	if err != nil {
		return nil, err
	}

	footerOffset := int(fileSize - footerSizeInBytes)
	numOffsets := int(binary.LittleEndian.Uint32(buf[footerOffset:]))
	indexLength := numOffsets * 4
	indexOffset := footerOffset - indexLength
	indexBuf := buf[indexOffset : indexOffset+indexLength]

	low, high := 0, numOffsets
	var mid int
	for low < high {
		mid = (low + high) / 2
		offset := int(binary.LittleEndian.Uint32(indexBuf[mid*4 : mid*4+4]))
		keyLen, n := binary.Uvarint(buf[offset:])
		offset += n
		valLen, n := binary.Uvarint(buf[offset:])
		offset += n
		key := buf[offset : offset+int(keyLen)]
		offset += int(keyLen)
		val := buf[offset : offset+int(valLen)]
		cmp := bytes.Compare(searchKey, key)
		switch {
		case cmp > 0:
			low = mid + 1
		case cmp < 0:
			high = mid
		case cmp == 0:
			return r.encoder.Decode(val), nil
		}
	}
	return nil, ErrorKeyNotFound
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
