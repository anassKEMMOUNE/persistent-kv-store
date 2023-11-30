package main

func main() {

	memtable := NewMemTable()
	if !memtable.wal.IsWalEmpty() {
		memtable.RecoverFromWAL()
	}

	r := SetupAPI(*memtable)
	r.Run(":8080")

}
