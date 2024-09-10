package lsm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMemtable_Insert(t *testing.T) {
	memtable := NewMemtable(1024, false)
	key := []byte("testkey")
	value := []byte("testValue")
	memtable.Insert(key, value)
	encodedValue, err := memtable.Get(key)
	if err != nil {
		t.Fatal("key not found in the memtable")
	}
	realValue := encodedValue.Value()
	assert.Equal(t, value, realValue)
}

func TestMemtable_InsertTombstone(t *testing.T) {
	memtable := NewMemtable(1024, false)
	key := []byte("testkey")
	memtable.Insert(key, []byte("testValue"))
	memtable.InsertTombstone(key)
	encodedValue, err := memtable.Get(key)
	if err != nil {
		t.Fatal("key not found in the memtable")
	}
	assert.Equal(t, true, encodedValue.IsTombstone())
}

func TestMemtable_InsertTombstone2(t *testing.T) {
	memtable := NewMemtable(1024, false)
	key := []byte("testkey")
	memtable.Insert(key, []byte("testValue"))
	memtable.InsertTombstone(key)
	memtable.Insert(key, []byte("testValue"))
	encodedValue, err := memtable.Get(key)
	if err != nil {
		t.Fatal("key not found in the memtable")
	}
	assert.Equal(t, false, encodedValue.IsTombstone())
	assert.Equal(t, []byte("testValue"), encodedValue.Value())
}

func TestMemtable_HasRoomForWrite(t *testing.T) {
	memtable := NewMemtable(1024, false)
	assert.Equal(t, true, memtable.HasRoomForWrite([]byte("testKey"), []byte("testValue")))
	i := 0
	for memtable.Size() < 1024 {
		i++
		key := []byte(fmt.Sprintf("testKey%d", i))
		value := []byte(fmt.Sprintf("testValue%d", i))
		memtable.Insert(key, value)
	}
	key := []byte(fmt.Sprintf("testKey%d", i+1))
	value := []byte(fmt.Sprintf("testValue%d", i+1))
	assert.Equal(t, false, memtable.HasRoomForWrite(key, value))
}
