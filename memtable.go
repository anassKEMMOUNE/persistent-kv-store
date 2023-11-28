package main

import "sync"

type KeyValue struct {
	Key   string
	Value string
}
type MemTable struct {
	data map[string]string
	mu   sync.RWMutex
	wal Wal
}

func NewMemTable() MemTable {
	return MemTable{data: make(map[string]string),wal : openWal()}
}

func (mt *MemTable) GetData() map[string]string {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.data
}
func (mt *MemTable) Set(key, value string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.wal.writeSetWal(key,value)
	mt.data[key] = value
}

func (mt *MemTable) Get(key string) (string, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	value, exists := mt.data[key]
	return value, exists
}

func (mt *MemTable) Delete(key string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.wal.writeDelWal(key)
	delete(mt.data, key)
}
