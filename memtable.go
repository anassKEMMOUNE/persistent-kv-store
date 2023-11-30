package main

import (
	"fmt"
	"os"
)

type KeyValue struct {
	Key   string
	Value string
}

type MemTable struct {
	data      map[string]string
	wal       Wal
	SSTables  []string
	flushSize int
}

func NewMemTable() *MemTable {
	return &MemTable{
		data:      make(map[string]string),
		wal:       openWal(),
		SSTables:  make([]string, 0),
		flushSize: 5,
	}
}

func (mt *MemTable) GetData() map[string]string {

	return mt.data
}

func (mt *MemTable) Set(key, value string) {

	mt.wal.writeSetWal(key, value)
	mt.data[key] = value
	if len(mt.data) >= mt.flushSize {
		// Flush the MemTable to SST file
		flushHandler := NewFlushHandler(mt, "sst_files")
		if err := flushHandler.Flush(); err != nil {
			fmt.Println("Error flushing MemTable:", err)
			return
		}
		ClearWal()
	}
}

func (mt *MemTable) Get(key string) (string, bool) {
	// Check in-memory data first
	value, exists := mt.data[key]
	if exists {
		return value, true
	}

	// If not found in MemTable, seek in SST files
	for _, sstFilePath := range mt.SSTables {
		sstFile, err := os.Open(sstFilePath)
		if err != nil {
			fmt.Println("Error opening SST file:", err)
			continue
		}
		defer sstFile.Close()

		foundValue, err := seekSSTFile(sstFile, key)
		if err != nil {
			fmt.Println("Error seeking in SST file:", err)
			continue
		}

		if foundValue != "" {
			return foundValue, true
		}
	}

	// Key not found in MemTable and SST files
	return "", false
}

func (mt *MemTable) Delete(key string) {

	mt.wal.writeDelWal(key)
	delete(mt.data, key)
}

func (mt *MemTable) Clear() {

	mt.data = make(map[string]string)
}

func (mt *MemTable) RecoverFromWAL() error {
	entries, err := mt.wal.readAllWal()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		switch entry.Operation {
		case "SET":
			mt.data[entry.Key] = entry.Value
		case "DELETE":
			delete(mt.data, entry.Key)
		}
	}

	return nil
}
