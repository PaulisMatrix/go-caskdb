package main

import (
	"fmt"
)

func main() {
	store, _ := NewDiskStore("books.db")
	store.Set("othello", "shakespeare1")

	//keys are just appened to the log file
	//but still we see the updated value since the in-memory hash table stores the updated offset for the same key.
	store.Set("othello", "shakespeare2")
	author2 := store.Get("othello")
	fmt.Printf("value read %s\n", author2)

	store.Close()
}
