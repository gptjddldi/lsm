package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/gptjddldi/lsm/db/regression"
)

type Index struct {
	entries []IndexEntry
	learned *regression.LinearRegression
}

type IndexEntry struct {
	key   []byte
	value []byte
}

func NewIndex(indexBytes []byte) *Index {
	var entries []IndexEntry
	buf := bytes.NewBuffer(indexBytes)

	for {
		keyLen, _ := binary.ReadUvarint(buf)
		if keyLen == 0 {
			break
		}
		valLen, _ := binary.ReadUvarint(buf)
		if valLen == 0 {
			break
		}
		valLen -= 1 // subtract the added 1

		key := make([]byte, keyLen)
		buf.Read(key)

		// Skip the operation type byte
		buf.ReadByte()

		val := make([]byte, valLen)
		buf.Read(val)

		entries = append(entries, IndexEntry{key: key, value: val})
	}

	// train 방식 최적화 가능
	x := make([]uint64, len(entries))
	y := make([]uint64, len(entries))
	for i, entry := range entries {
		str, _ := stringToInt(string(entry.key))
		x[i] = str
		y[i] = uint64(i)
		// TODO: mode 에 따라서 들어가는 key 가 바뀌어야 함.
	}
	b := regression.NewRegression()
	b.Train(x, y)

	return &Index{entries: entries, learned: b}
}

func (idx *Index) Get(searchKey []byte) IndexEntry {
	low, high := 0, len(idx.entries)-1
	var mid int
	for low < high {
		mid = (low + high) / 2
		cmp := bytes.Compare(searchKey, idx.entries[mid].key)
		if cmp > 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	key, _ := stringToInt(string(searchKey))
	predicted := idx.learned.Predict(uint64(key))
	fmt.Println(low, predicted)

	return idx.entries[low]
}

// stringToInt: 6자리 이하 소문자+숫자로 이루어진 스트링 변환
// 결과물 간의 비교는 bytes.Compare 과 동일함 (based on lexicographic)
func stringToInt(s string) (uint64, error) {
	if len(s) == 0 {
		return 0, nil
	}

	if len(s) > 6 {
		return 0, fmt.Errorf("string should be less than 6 characters")
	}

	base := uint64(36) // 10 (digits) + 26 (lowercase)
	charToValue := make(map[rune]uint64)

	for i := 0; i < 10; i++ {
		charToValue[rune('0'+i)] = uint64(i)
	}

	for i := 0; i < 26; i++ {
		charToValue[rune('a'+i)] = uint64(i + 10)
	}

	// 6 자리 미만에 zero padding 추가
	for i := len(s); i < 6; i++ {
		s = s + "0"
	}

	var result uint64 = 0
	for _, char := range s {
		value, exists := charToValue[char]
		if !exists {
			return 0, fmt.Errorf("invalid character is contained")
		}
		result = result*base + value
	}

	return result, nil
}
