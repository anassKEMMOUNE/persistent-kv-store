package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

func seekSSTFile(file *os.File, key string) (string, error) {
	// Read the SST header to get the key range
	entryCount, smallestKey, largestKey, err := readSSTHeader(file)
	if err != nil {
		return "", fmt.Errorf("error reading SST header: %v", err)
	}

	// Binary search for the key in the SST file
	foundValue, err := binarySearchSST(file, key, entryCount, smallestKey, largestKey)
	if err != nil {
		return "", fmt.Errorf("error performing binary search: %v", err)
	}

	return foundValue, nil
}

func readSSTHeader(file *os.File) (int, string, string, error) {
	// Read magic number
	var magic uint32
	if err := binary.Read(file, binary.LittleEndian, &magic); err != nil {
		return 0, "", "", err
	}
	if magic != 0x12345678 {
		return 0, "", "", fmt.Errorf("invalid magic number in SST file")
	}

	// Read entry count
	var entryCount uint32
	if err := binary.Read(file, binary.LittleEndian, &entryCount); err != nil {
		return 0, "", "", err
	}

	// Read smallest key
	smallestKey, err := readString(file)
	if err != nil {
		return 0, "", "", err
	}

	// Read largest key
	largestKey, err := readString(file)
	if err != nil {
		return 0, "", "", err
	}

	// Version is not used in this example, but you might need it for future versions
	var version uint16
	if err := binary.Read(file, binary.LittleEndian, &version); err != nil {
		return 0, "", "", err
	}

	return int(entryCount), smallestKey, largestKey, nil
}

func binarySearchSST(file *os.File, key string, entryCount int, smallestKey, largestKey string) (string, error) {
	low, high := 0, entryCount-1

	for low <= high {
		mid := (low + high) / 2

		// Read the mid key to compare with the target key
		midKey, _, err := readKeyValuePairAt(file, mid)
		if err != nil {
			return "", err
		}

		if midKey == key {
			// Key found, read and return the corresponding value
			_, midValue, err := readKeyValuePairAt(file, mid)
			if err != nil {
				return "", err
			}
			return midValue, nil
		} else if midKey < key {
			// Adjust the search range
			low = mid + 1
		} else {
			// midKey > key
			// Adjust the search range
			high = mid - 1
		}
	}

	// Key not found in the SST file
	return "", nil
}

func readKeyValuePairAt(file *os.File, index int) (string, string, error) {
	// Read header offset
	headerOffset := int64(4 + 4 + 4 + 4 + 4 + 2) // Magic + EntryCount + SmallestKeyLen + LargestKeyLen + Version

	// Read each key-value pair's offset
	keyValueOffset := headerOffset + int64(index)*(4 + 4) // KeyLen + ValueLen

	// Seek to the offset of the key-value pair
	_, err := file.Seek(keyValueOffset, 0)
	if err != nil {
		return "", "", err
	}

	// Read and return the key-value pair
	return readKeyValuePair(file)
}

func readKeyValuePair(file *os.File) (string, string, error) {
	// Read key length
	var keyLen uint32
	if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
		return "", "", err
	}

	// Read key
	key, err := readStringN(file, int(keyLen))
	if err != nil {
		return "", "", err
	}

	// Read value length
	var valueLen uint32
	if err := binary.Read(file, binary.LittleEndian, &valueLen); err != nil {
		return "", "", err
	}

	// Read value
	value, err := readStringN(file, int(valueLen))
	if err != nil {
		return "", "", err
	}

	return key, value, nil
}

func readString(file *os.File) (string, error) {
	// Read string length
	var strLen uint32
	if err := binary.Read(file, binary.LittleEndian, &strLen); err != nil {
		return "", err
	}

	// Read string
	return readStringN(file, int(strLen))
}

func readStringN(file *os.File, n int) (string, error) {
	// Read exactly n bytes from the file
	data := make([]byte, n)
	_, err := file.Read(data)
	if err != nil {
		return "", err
	}

	return string(data), nil
}
