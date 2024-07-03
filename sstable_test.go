package lsm

import (
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/gptjddldi/lsm/db/storage"
	"github.com/stretchr/testify/assert"
)

const (
	N = 1000
)

func TestSstable_Get(t *testing.T) {
	fileName, err := generateSSTable2()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0644)

	sst := NewSSTable(f)
	value, err := sst.Get([]byte("testKey1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("testValue1"), value.Value())

	value, err = sst.Get([]byte(fmt.Sprint("testKey", N)))
	assert.NoError(t, err)
	assert.Equal(t, []byte(fmt.Sprint("testValue", N)), value.Value())

	value, err = sst.Get([]byte(fmt.Sprint("testKey", N+1)))
	assert.EqualError(t, err, "key not found")

	os.Remove(f.Name())
}

func TestSSTable_Iterator(t *testing.T) {
	fileName, err := generateSSTable2()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	keys := sortedKeys()
	sst := NewSSTable(f)
	iter := sst.Iterator()
	for i := 0; i < 1000; i++ {
		ok, _ := iter.Next()
		if !ok {
			break
		}

		assert.Equal(t, []byte(keys[i]), iter.Key())
	}

	os.Remove(f.Name())
}

func TestSSTableIsInKeyRange(t *testing.T) {
	fileName, err := generateSSTable2()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	sst := NewSSTable(f)
	assert.True(t, sst.IsInKeyRange([]byte("testKey1"), []byte(fmt.Sprintf("testKey%d", N))))
	assert.True(t, sst.IsInKeyRange([]byte("testKey0"), []byte(fmt.Sprintf("testKey9999%d", N+1))))
	assert.False(t, sst.IsInKeyRange([]byte(fmt.Sprintf("testKey9999%d", N+1)), []byte(fmt.Sprintf("testKey99999%d", N+2))))
	assert.True(t, sst.IsInKeyRange([]byte("testKey0"), []byte(fmt.Sprintf("testKey1"))))
	os.Remove(f.Name())
}

func TestSSTable_MinMaxKey(t *testing.T) {
	fileName, err := generateSSTable2()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	sst := NewSSTable(f)
	assert.Equal(t, []byte("testKey1"), sst.minKey)
	assert.Equal(t, []byte("testKey999"), sst.maxKey)

	os.Remove(f.Name())
}

func TestSSTable_Contains(t *testing.T) {
	fileName, err := generateSSTable2()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	sst := NewSSTable(f)
	for i := 1; i <= N; i++ {
		assert.True(t, sst.Contains([]byte(fmt.Sprintf("testKey%d", i))))
	}
	for i := N + 1; i <= 2*N; i++ {
		assert.False(t, sst.Contains([]byte(fmt.Sprintf("testKey%d", i))))
	}
	os.Remove(f.Name())
}

func generateSSTable2() (string, error) {
	memtable := NewMemtable(100000)
	i := 0
	for i < N {
		i++
		key := []byte(fmt.Sprintf("testKey%d", i))
		value := []byte(fmt.Sprintf("testValue%d", i))
		memtable.Insert(key, value)
	}

	provider, err := storage.NewProvider("./test/")
	if err != nil {
		return "", err
	}
	meta := provider.PrepareNewFile(0)
	f, err := provider.OpenFileForWriting(meta)
	if err != nil {
		return "", err
	}
	flusher := NewFlusher(memtable, f)
	err = flusher.Flush()
	if err != nil {
		return "", err
	}
	return f.Name(), nil
}

func sortedKeys() []string {
	keys := make([]string, N) // Initialize the slice with length N
	for i := 0; i < N; i++ {
		keys[i] = fmt.Sprintf("testKey%d", i+1)
	}
	sort.Strings(keys)
	return keys
}
