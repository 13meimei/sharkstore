package main

import (
	"os"
	"fmt"
	ts "model/pkg/timestamp"
	"util"
	"math/rand"
	"time"
	"runtime"
	"master-server/engine/boltstore"
	"path/filepath"
	"encoding/binary"
	"sync"
)

var Path1 = "/export/Data/Row/Data"
var Path2 = "/export/Data/Bolt/Data"
var Path3 = "/export/Data/Bolt1/Data"

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)

func RandomBytes(size int) []byte {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	buff := make([]byte, size)
	util.RandomBytes(r, buff)
	return buff
}

func BoltStore(path string) error {
	file := filepath.Join(path, "test.db")
	store, _, err := boltstore.NewBoltStore(file)
	if err != nil {
		fmt.Println("open store failed ", err)
		os.Exit(-1)
	}
	defer store.Close()
	start := time.Now()
	var buff [8]byte
	var wg   sync.WaitGroup
	var i uint64
	for i = 0; i < uint64(1024); i++ {
		wg.Add(1)
		go func(index uint64) {
			defer wg.Done()
			var count uint64 = 1
			size := 0
			batch := store.NewWriteBatch()
			for {
				var key, value []byte
				binary.BigEndian.PutUint64(buff[:], uint64(index << 32 + count))
				key = append(key, []byte("key_")...)
				key = append(key, buff[:]...)
				value = append(value, []byte("value_")...)
				value = append(value, RandomBytes(1024)...)
				batch.Put(key, value, 0, ts.Timestamp{WallTime: int64(count)}, uint64(count))
				count++
				if count % uint64(1000) == 0 {
					fmt.Println("---------------put ", count)
				}
				size += len(key)
				size += len(value)
				if size >= MB {
					break
				}
			}
			batch.Commit()
		}(i)
	}
	wg.Wait()
	fmt.Println("&&&&&&&&&&&&&&&&&&blot put KV success, time ", time.Since(start).String())
	iter := store.NewIterator([]byte("key_"), []byte("kez"), ts.MaxTimestamp)
	defer iter.Release()
	start = time.Now()
	for iter.Next() {
		iter.Key()
		iter.Value()
	}
	fmt.Println("&&&&&&&&&&&&&&&&&&&blot get KV success, time ", time.Since(start).String())
	return nil
}

func BoltStore1(path string) error {
	file := filepath.Join(path, "test.db")
	store, _, err := boltstore.NewBoltStore(file)
	if err != nil {
		fmt.Println("open store failed ", err)
		os.Exit(-1)
	}
	defer store.Close()
	start := time.Now()
	count := 1
	size := 0
	var buff [8]byte
	for {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(count))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, RandomBytes(1024)...)
		store.Put(key, value, 0, ts.Timestamp{WallTime: int64(count)}, uint64(count))
		count++
		if count % 1000 == 0 {
			fmt.Println("---------------put ", count)
		}
		size += len(key)
		size += len(value)
		if size >= GB {
			break
		}
	}
	fmt.Println("&&&&&&&&&&&&&&&&&&blot put KV success, time ", time.Since(start).String())
	iter := store.NewIterator([]byte("key_"), []byte("kez"), ts.MaxTimestamp)
	defer iter.Release()
	start = time.Now()
	for iter.Next() {
		iter.Key()
		iter.Value()
	}
	fmt.Println("&&&&&&&&&&&&&&&&&&&blot get KV success, time ", time.Since(start).String())
	return nil
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() - 1)
	os.RemoveAll(Path1)
	os.RemoveAll(Path2)
	os.RemoveAll(Path3)
	err := os.MkdirAll(Path1, 0755)
	if err != nil {
		fmt.Printf("make dir failed, err[%v]\n", err)
		return
	}
	err = os.MkdirAll(Path2, 0755)
	if err != nil {
		fmt.Printf("make dir failed, err[%v]\n", err)
		return
	}
	err = os.MkdirAll(Path3, 0755)
	if err != nil {
		fmt.Printf("make dir failed, err[%v]\n", err)
		return
	}
	//err = RowStore(Path1)
	//if err != nil {
	//	fmt.Println("row store test failed, err ", err)
	//	return
	//}
	//fmt.Println("************start bolt store*******************")
	//err = BoltStore(Path2)
	//if err != nil {
	//	fmt.Println("row store test failed, err ", err)
	//	return
	//}
	fmt.Println("************start bolt store 1*******************")
	err = BoltStore1(Path3)
	if err != nil {
		fmt.Println("row store test failed, err ", err)
		return
	}
}
