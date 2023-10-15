package main

import "fmt"

func main() {
	store, _ := NewDiskStore("books.db")
	store.Set("othello", "shakespeare")
	author1 := store.Get("othello")
	fmt.Printf("value read %s\n", author1)

	store.Set("war and peace", "tolstoy")
	author2 := store.Get("war and peace")
	fmt.Printf("value read %s\n", author2)

	store.Close()
}
