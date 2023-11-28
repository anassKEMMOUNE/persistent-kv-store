package main

import "fmt"

func main() {
	memTable := NewMemTable()
	memTable.Set("anass", "2")
	memTable.Set("zakaria", "3")
	memTable.Set("ayman", "4")
	memTable.Set("abdo", "5")
	value,exists := memTable.Get("anass")
	fmt.Print(value," ",exists)
	memTable.Delete("anass")
	value,exists = memTable.Get("anass")
	fmt.Print(value," ",exists)


}
