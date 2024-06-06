package lsm

import (
	"fmt"
	"github.com/gptjddldi/lsm/db/storage"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestFlusher_Flush(t *testing.T) {
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
		t.Fatal(err)
	}
	meta := provider.PrepareNewFile(0)
	f, err := provider.OpenFileForWriting(meta)
	if err != nil {
		t.Fatal(err)
	}
	flusher := NewFlusher(memtable, f)
	err = flusher.Flush()
	if err != nil {
		t.Fatal(err)
	}
	fs, _ := f.Stat()
	assert.GreaterOrEqual(t, fs.Size(), int64(1024))

	os.Remove(f.Name())
}
