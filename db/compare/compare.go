package compare

import (
	"bytes"
)

func Compare(a, b []byte, number bool) int {
	if number {
		numA := ByteToInt(a)
		numB := ByteToInt(b)

		if numA > numB {
			return 1
		} else if numA < numB {
			return -1
		} else {
			return 0
		}
	}
	return bytes.Compare(a, b) // Default byte comparison
}

func ByteToInt(b []byte) int {
	result := 0
	for _, v := range b {
		result = result*10 + int(v-'0')
	}
	return result
}
