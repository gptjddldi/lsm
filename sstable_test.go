package lsm

import (
	"fmt"
	"github.com/gptjddldi/lsm/db/storage"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestSstable_Get(t *testing.T) {
	err := generateSSTable2()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join("./test", "000001.sst"), os.O_RDONLY, 0644)

	sst := NewSSTable(f)
	value, err := sst.Get([]byte("testKey27"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("testValue27"), value.Value())

	value, err = sst.Get([]byte("testKey270"))
	assert.EqualError(t, err, "key not found")

	os.Remove(f.Name())
}

func generateSSTable2() error {
	memtable := NewMemtable(1024)
	i := 0
	for memtable.Size() < 1024 {
		i++
		key := []byte(fmt.Sprintf("testKey%d", i))
		value := []byte(fmt.Sprintf("testValue%d", i))
		memtable.Insert(key, value)
	}

	provider, err := storage.NewProvider("./test/")
	if err != nil {
		return err
	}
	meta := provider.PrepareNewFile()
	f, err := provider.OpenFileForWriting(meta)
	if err != nil {
		return err
	}
	flusher := NewFlusher(memtable, f)
	err = flusher.Flush()
	if err != nil {
		return err
	}
	return nil
}
