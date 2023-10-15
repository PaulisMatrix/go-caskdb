package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"
)

// DiskStore is a Log-Structured Hash Table as described in the BitCask paper. We
// keep appending the data to a file, like a log. DiskStorage maintains an in-memory
// hash table called KeyDir, which keeps the row's location on the disk.
//
// The idea is simple yet brilliant:
//   - Write the record to the disk
//   - Update the internal hash table to point to that byte offset
//   - Whenever we get a read request, check the internal hash table for the address,
//     fetch that and return
//
// KeyDir does not store values, only their locations.
//
// The above approach solves a lot of problems:
//   - Writes are insanely fast since you are just appending to the file
//   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
//     storage, there could be 2-3 disk seeks
//
// However, there are drawbacks too:
//   - We need to maintain an in-memory hash table KeyDir. A database with a large
//     number of keys would require more RAM
//   - Since we need to build the KeyDir at initialisation, it will affect the startup
//     time too
//   - Deleted keys need to be purged from the file to reduce the file size
//
// Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf
//
// DiskStore provides two simple operations to get and set key value pairs. Both key
// and value need to be of string type, and all the data is persisted to disk.
// During startup, DiskStorage loads all the existing KV pair metadata, and it will
// throw an error if the file is invalid or corrupt.
//
// Note that if the database file is large, the initialisation will take time
// accordingly. The initialisation is also a blocking operation; till it is completed,
// we cannot use the database.
//
// Typical usage example:
//
//		store, _ := NewDiskStore("books.db")
//	   	store.Set("othello", "shakespeare")
//	   	author := store.Get("othello")
type DiskStore struct {
	file     *os.File
	writePos uint32
	KeyDir   map[string]KeyEntry
}

func isFileExists(fileName string) bool {
	// https://stackoverflow.com/a/12518877
	if _, err := os.Stat(fileName); err == nil || errors.Is(err, fs.ErrExist) {
		return true
	}
	return false
}

func NewDiskStore(fileName string) (*DiskStore, error) {
	ds := &DiskStore{KeyDir: make(map[string]KeyEntry)}

	if isFileExists(fileName) {
		ds.LoadKeys(fileName)
	}

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return &DiskStore{}, err
	}
	ds.file = file
	return ds, nil
}

func (d *DiskStore) LoadKeys(fileName string) {
	//load all keys from the file in in-mem hashtable
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("error in loading keys from the file db", err)
		os.Exit(1)
	}

	for {
		buffer := make([]byte, headerSize)
		_, err := file.Read(buffer)
		if err == io.EOF {
			fmt.Println("done reading from the file. exiting...")
			break
		}
		if err != nil {
			fmt.Println("unkown error while reading the file", err)
			break
		}
		timestamp, keySize, valueSize := decodeHeader(buffer)
		key := make([]byte, keySize)
		value := make([]byte, valueSize)

		//Read automatically updates the offset to point to next byte to read from
		_, err = file.Read(key)
		if err != nil {
			fmt.Println("error in reading keys", err)
			break
		}
		_, err = file.Read(value)
		if err != nil {
			fmt.Println("error in reading values", err)
			break
		}
		fmt.Printf("loaded key=%s and value=%s\n", string(key), string(value))
		totalSize := headerSize + keySize + valueSize
		d.KeyDir[string(key)] = KeyEntry{totalSize: totalSize, writeOffSet: d.writePos, timestamp: timestamp}
		d.writePos += totalSize
	}

}

func (d *DiskStore) Get(key string) string {
	keyEntry, ok := d.KeyDir[key]
	if !ok {
		//key is not present, create first
		return ""
	}
	writeOffset, totalSize := keyEntry.writeOffSet, keyEntry.totalSize
	//before reading the kv we need to seek to the correct offset
	_, err := d.file.Seek(int64(writeOffset), io.SeekStart)
	if err != nil {
		fmt.Println("error in seeking to the correct offset", err)
		os.Exit(1)
	}
	buffer := make([]byte, totalSize)
	_, err = d.file.Read(buffer)
	if err != nil {
		fmt.Println("error in reading the kv", err)
		os.Exit(1)
	}
	_, _, value := decodeKV(buffer)
	return value
}

func (d *DiskStore) Set(key string, value string) {
	timestamp := uint32(time.Now().Unix())
	totalSize, data := encodeKV(timestamp, key, value)
	_, err := d.file.Write(data)
	if err != nil {
		fmt.Println("error while writing kv to disk", err)
		os.Exit(1)
	}
	d.KeyDir[key] = KeyEntry{timestamp: timestamp, writeOffSet: d.writePos, totalSize: uint32(totalSize)}
	//update the writeOffset
	d.writePos += uint32(totalSize)
}

func (d *DiskStore) Close() {
	err := d.file.Close()
	if err != nil {
		fmt.Println("error in closing the file", err)
		os.Exit(1)
	}
}
