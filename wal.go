package main

import ("os" 
"fmt")

type Wal struct {
	LogFile *os.File
	Err     error
}

func openWal() Wal {
	logFile, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	return Wal{
		LogFile:    logFile,
		Err: err,
	}
}

func (wal *Wal) writeSetWal(key, value string){
	if _, err := wal.LogFile.WriteString(fmt.Sprintf("SET %s %s\n", key, value)); err != nil {
		fmt.Println("Error writing to WAL:", err)
		return
	}
}

func (wal *Wal) writeDelWal(key string){
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


// func (mt *MemTable) RecoverFromWAL() {
// 	mt.mu.Lock()
// 	defer mt.mu.Unlock()

// 	// Seek to the beginning of the log file
// 	_, err := mt.logFile.Seek(0, 0)
// 	if err != nil {
// 		fmt.Println("Error seeking to the beginning of the log file:", err)
// 		return
// 	}

// 	// Read and replay each entry from the log file
// 	scanner := bufio.NewScanner(mt.logFile)
// 	for scanner.Scan() {
// 		entry := scanner.Text()
// 		parts := strings.Fields(entry)

// 		if len(parts) < 2 {
// 			fmt.Println("Invalid log entry:", entry)
// 			continue
// 		}

// 		switch parts[0] {
// 		case "PUT":
// 			if len(parts) == 3 {
// 				mt.data[parts[1]] = parts[2]
// 			} else {
// 				fmt.Println("Invalid PUT log entry:", entry)
// 			}
// 		case "DELETE":
// 			if len(parts) == 2 {
// 				delete(mt.data, parts[1])
// 			} else {
// 				fmt.Println("Invalid DELETE log entry:", entry)
// 			}
// 		default:
// 			fmt.Println("Unknown log entry:", entry)
// 		}
// 	}

// 	if err := scanner.Err(); err != nil {
// 		fmt.Println("Error reading log file:", err)
// 	}
// }