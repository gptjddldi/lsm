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
	meta := provider.PrepareNewFile()
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
