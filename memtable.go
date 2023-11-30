package main

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
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
	ind       int
	flushSize int
}

func NewMemTable() *MemTable {
	return &MemTable{
		data:      make(map[string]string),
		wal:       openWal(),
		SSTables:  make([]string, 0),
		ind:       0,
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
		mt.ind++
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
	fmt.Println(len(mt.SSTables))
	// Check in-memory data first
	if value, exists := mt.data[key]; exists {
		return value, true
	}

	// If not in memory, check SST files
	folderPath := "sst_files"

	// Read the contents of the folder
	files, err := ioutil.ReadDir(folderPath)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		sstFile := "sst_files/" + file.Name()
		fmt.Println("evfb")
		if value, exists := readFromSST(sstFile, key); exists {
			return value, true
		}
	}

	// Key not found
	return "", false
}

func readFromSST(filePath, key string) (string, bool) {
	fmt.Println("hhh")
	file, err := os.Open(filePath)

	if err != nil {
		fmt.Printf("Error opening SST file: %v\n", err)
		return "", false
	}
	defer file.Close()

	// Read and skip the SST header
	var magicNumber, entryCount, smallestKeyLen, largestKeyLen uint32
	var version uint16

	if err := binary.Read(file, binary.LittleEndian, &magicNumber); err != nil {
		return "", false
	}
	if err := binary.Read(file, binary.LittleEndian, &entryCount); err != nil {
		return "", false
	}
	if err := binary.Read(file, binary.LittleEndian, &smallestKeyLen); err != nil {
		return "", false
	}
	if _, err := file.Seek(int64(smallestKeyLen), os.SEEK_CUR); err != nil { // Skip smallest key
		return "", false
	}
	if err := binary.Read(file, binary.LittleEndian, &largestKeyLen); err != nil {
		return "", false
	}
	if _, err := file.Seek(int64(largestKeyLen), os.SEEK_CUR); err != nil { // Skip largest key
		return "", false
	}
	if err := binary.Read(file, binary.LittleEndian, &version); err != nil {
		return "", false
	}

	// Read through the file to find the key
	var keyLength, valueLength uint32
	for i := 0; i < int(entryCount); i++ {
		// Read the length of the key
		if err := binary.Read(file, binary.LittleEndian, &keyLength); err != nil {
			break // End of file or error
		}

		// Read the key
		currentKey := make([]byte, keyLength)
		if _, err := file.Read(currentKey); err != nil {
			break
		}

		// Read the length of the value
		if err := binary.Read(file, binary.LittleEndian, &valueLength); err != nil {
			break
		}

		// Read the value
		currentValue := make([]byte, valueLength)
		if _, err := file.Read(currentValue); err != nil {
			break
		}

		// Check if the current key is the one we're looking for
		if string(currentKey) == key {
			return string(currentValue), true
		}
	}

	// Key not found in this SST file
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
