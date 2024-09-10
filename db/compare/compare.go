package compare

import (
	"bytes"
)

func Compare(a, b []byte, number bool) int {
	if !number {
		return bytes.Compare(a, b)
	}

	if len(a) < len(b) {
		return -1
	} else if len(a) > len(b) {
		return 1
	}

	for i := 0; i < len(a); i++ {
		if a[i] < b[i] {
			return -1
		} else if a[i] > b[i] {
			return 1
		}
	}

	return 0
}

func ByteToInt(b []byte) int {
	result := 0
	for _, v := range b {
		result = result*10 + int(v-'0')
	}
	return result
}
