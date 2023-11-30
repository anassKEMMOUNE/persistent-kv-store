package main

import (
	"fmt"
	"os"
	"strings"
)

type Wal struct {
	LogFile *os.File
	Err     error
}
type WalEntry struct {
	Operation string
	Key       string
	Value     string
}

func openWal() Wal {
	logFile, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	return Wal{
		LogFile: logFile,
		Err:     err,
	}
}

func (wal *Wal) writeSetWal(key, value string) {
	if _, err := wal.LogFile.WriteString(fmt.Sprintf("SET %s %s\n", key, value)); err != nil {
		fmt.Println("Error writing to WAL:", err)
		return
	}
}

func (wal *Wal) writeDelWal(key string) {
	// Write to WAL
	if _, err := wal.LogFile.WriteString(fmt.Sprintf("DELETE %s\n", key)); err != nil {
		fmt.Println("Error writing to WAL:", err)
		return
	}
}

func (wal *Wal) CloseLogFile() {
	if err := wal.LogFile.Close(); err != nil {
		fmt.Println("Error closing WAL:", err)
	}
}
func ClearWal()  {
	err := os.Truncate("wal.log", 0)
	if err != nil {
		fmt.Println("Error:", err)
	}

}

func (wal *Wal) IsWalEmpty() bool {
	// Get file info to check the size
	fileInfo, err := wal.LogFile.Stat()
	if err != nil {
		fmt.Println("Error getting file info:", err)
		return true
	}

	// Check if the size of the file is zero
	return fileInfo.Size() == 0
}

func (wal *Wal) readAllWal() ([]WalEntry, error) {
	var entries []WalEntry

	// Read the entire file into a byte slice
	content, err := os.ReadFile("wal.log")
	if err != nil {
		fmt.Println("Error reading WAL file:", err)
		return nil, err
	}

	// Split the content into lines
	lines := strings.Split(string(content), "\n")

	for _, line := range lines {
		// Skip empty lines
		if line == "" {
			continue
		}

		entry, err := parseWalEntry(line)
		if err != nil {
			fmt.Println("Error parsing WAL entry:", err)
			continue
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// Helper function to parse a WAL entry from a string
func parseWalEntry(line string) (WalEntry, error) {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return WalEntry{}, fmt.Errorf("invalid WAL entry: %s", line)
	}

	operation := parts[0]
	key := parts[1]
	var value string
	if operation == "SET" && len(parts) >= 3 {
		value = parts[2]
	}

	return WalEntry{
		Operation: operation,
		Key:       key,
		Value:     value,
	}, nil
}
