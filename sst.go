package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"
)

type FlushHandler struct {
	memTable *MemTable
	sstDir   string
	mu       sync.Mutex
}

func NewFlushHandler(memTable *MemTable, sstDir string) *FlushHandler {
	return &FlushHandler{
		memTable: memTable,
		sstDir:   sstDir,
	}
}

func (fh *FlushHandler) Flush() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	// Create a new SST file
	sstFilePath := fmt.Sprintf("%s/sst_%d.sst", fh.sstDir, len(fh.memTable.SSTables)+1)
	sstFile, err := os.Create(sstFilePath)
	if err != nil {
		return fmt.Errorf("error creating SST file: %v", err)
	}
	defer sstFile.Close()

	// Sort keys for deterministic order
	keys := make([]string, 0, len(fh.memTable.data))
	for k := range fh.memTable.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Write header information to the SST file
	if err := writeSSTHeader(sstFile, len(keys), keys[0], keys[len(keys)-1]); err != nil {
		return fmt.Errorf("error writing SST header: %v", err)
	}

	// Write key-value pairs to the SST file
	for _, key := range keys {
		value := fh.memTable.data[key]
		if err := writeKeyValue(sstFile, key, value); err != nil {
			return fmt.Errorf("error writing key-value pair: %v", err)
		}
	}

	// Add the new SST file to the list
	fh.memTable.SSTables = append(fh.memTable.SSTables, sstFilePath)

	// Clear the MemTable
	fh.memTable.Clear()

	return nil
}

func writeSSTHeader(file *os.File, entryCount int, smallestKey, largestKey string) error {
	// Magic number: 4 bytes
	if err := binary.Write(file, binary.LittleEndian, uint32(0x12345678)); err != nil {
		return err
	}

	// Entry count: 4 bytes
	if err := binary.Write(file, binary.LittleEndian, uint32(entryCount)); err != nil {
		return err
	}

	// Smallest key: 4 bytes
	if err := binary.Write(file, binary.LittleEndian, uint32(len(smallestKey))); err != nil {
		return err
	}
	if _, err := file.WriteString(smallestKey); err != nil {
		return err
	}

	// Largest key: 4 bytes
	if err := binary.Write(file, binary.LittleEndian, uint32(len(largestKey))); err != nil {
		return err
	}
	if _, err := file.WriteString(largestKey); err != nil {
		return err
	}

	// Version: 2 bytes
	if err := binary.Write(file, binary.LittleEndian, uint16(1)); err != nil {
		return err
	}

	return nil
}

func writeKeyValue(file *os.File, key, value string) error {
	// Write key length: 4 bytes
	if err := binary.Write(file, binary.LittleEndian, uint32(len(key))); err != nil {
		return err
	}
	// Write key: variable length
	if _, err := file.WriteString(key); err != nil {
		return err
	}

	// Write value length: 4 bytes
	if err := binary.Write(file, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}
	// Write value: variable length
	if _, err := file.WriteString(value); err != nil {
		return err
	}

	return nil
}
