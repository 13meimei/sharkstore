package boltstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"master-server/engine/errors"
	ts "model/pkg/timestamp"
)

var path = "/tmp/Data"

// func TestMy(t *testing.T) {
// 	store, _, err := NewBoltStore("/Users/gaojianlong/Downloads/fbase.db")
// 	if err != nil {
// 		t.Errorf("new store failed, err[%v]", err)
// 		return
// 	}
// 	defer store.Close()

// 	snap, err := store.GetSnapshot()
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	iter := snap.NewIterator([]byte("\x00"), []byte("\xff"))
// 	for iter.Next() {
// 		if iter.Error() != nil {
// 			t.Error(iter.Error())
// 		}
// 		key := iter.Key()
// 		t.Logf("got key: %v", key)
// 		if bytes.HasPrefix(key, []byte("schema")) {
// 			t.Logf("got key: %v", key)
// 			return
// 		}
// 	}
// 	t.Errorf("could not find schema db")
// }

func TestGet(t *testing.T) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		t.Errorf("make dir failed, err[%v]", err)
		return
	}
	defer os.RemoveAll(path)
	file := filepath.Join(path, "test.db")
	store, applyId, err := NewBoltStore(file)
	if err != nil {
		t.Errorf("new store failed, err[%v]", err)
		return
	}
	if applyId != 0 {
		t.Errorf("test failed %d", applyId)
		return
	}
	defer store.Close()
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err = store.Put(key, value, 0, ts.Timestamp{}, uint64(i))
		if err != nil {
			t.Errorf("put failed, err[%v]", err)
			return
		}
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		eValue := []byte(fmt.Sprintf("value_%d", i))
		value, err := store.Get(key, ts.Timestamp{})
		if err != nil {
			t.Errorf("get failed, err[%v]", err)
			return
		}
		if bytes.Compare(eValue, value) != 0 {
			t.Error("get failed")
			return
		}
	}

	value, err := store.Get([]byte("test"), ts.Timestamp{})
	if err != nil || value != nil {
		t.Error("test failed")
		return
	}
}

func TestDelete(t *testing.T) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		t.Errorf("make dir failed, err[%v]", err)
		return
	}
	defer os.RemoveAll(path)
	file := filepath.Join(path, "test.db")
	store, applyId, err := NewBoltStore(file)
	if err != nil {
		t.Errorf("new store failed, err[%v]", err)
		return
	}
	if applyId != 0 {
		t.Errorf("test failed %d", applyId)
		return
	}
	defer store.Close()
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err = store.Put(key, value, 0, ts.Timestamp{}, uint64(i))
		if err != nil {
			t.Errorf("put failed, err[%v]", err)
			return
		}
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		eValue := []byte(fmt.Sprintf("value_%d", i))
		value, err := store.Get(key, ts.Timestamp{})
		if err != nil {
			t.Errorf("get failed, err[%v]", err)
			return
		}
		if bytes.Compare(eValue, value) != 0 {
			t.Error("get failed")
		}
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		err := store.Delete(key, ts.Timestamp{}, uint64(i))
		if err != nil {
			t.Errorf("get failed, err[%v]", err)
			return
		}
		value, err := store.Get(key, ts.Timestamp{})
		if err != nil {
			t.Errorf("get failed, err[%v]", err)
			return
		}
		if len(value) != 0 {
			t.Error("test delete failed")
			return
		}
	}
}

func TestIter(t *testing.T) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		t.Errorf("make dir failed, err[%v]", err)
		return
	}
	defer os.RemoveAll(path)
	file := filepath.Join(path, "test.db")
	store, applyId, err := NewBoltStore(file)
	if err != nil {
		t.Errorf("new store failed, err[%v]", err)
		return
	}
	if applyId != 0 {
		t.Errorf("test failed %d", applyId)
		return
	}
	defer store.Close()
	var buff [8]byte
	for i := 0; i < 100; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		err = store.Put(key, value, 0, ts.Timestamp{}, uint64(i))
		if err != nil {
			t.Errorf("put failed, err[%v]", err)
			return
		}
	}
	var start, end []byte
	binary.BigEndian.PutUint64(buff[:], uint64(3))
	start = append(start, []byte("key_")...)
	start = append(start, buff[:]...)
	binary.BigEndian.PutUint64(buff[:], uint64(89))
	end = append(end, []byte("key_")...)
	end = append(end, buff[:]...)
	iter := store.NewIterator(start, end, ts.Timestamp{})
	if iter == nil {
		t.Error("new iter failed")
		return
	}
	defer iter.Release()
	count := 0
	for i := 3; iter.Next() != false; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		k := iter.Key()
		v := iter.Value()
		if bytes.Compare(key, k) != 0 || bytes.Compare(value, v) != 0 {
			t.Errorf("test iter failed [%s, %s] [%s, %s]", string(key), string(value), string(k), string(v))
			return
		}
		count++
	}
	if count != (89 - 3) {
		t.Errorf("test iter failed, count[%d], err[%v]", count, iter.Error())
		return
	}
}

func TestIter1(t *testing.T) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		t.Errorf("make dir failed, err[%v]", err)
		return
	}
	defer os.RemoveAll(path)
	file := filepath.Join(path, "test.db")
	store, applyId, err := NewBoltStore(file)
	if err != nil {
		t.Errorf("new store failed, err[%v]", err)
		return
	}
	if applyId != 0 {
		t.Errorf("test failed %d", applyId)
		return
	}
	defer store.Close()
	var buff [8]byte
	for i := 0; i < 100; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		err = store.Put(key, value, 0, ts.Timestamp{}, uint64(i))
		if err != nil {
			t.Errorf("put failed, err[%v]", err)
			return
		}
	}
	iter := store.NewIterator([]byte("key_"), []byte("kez"), ts.Timestamp{})
	if iter == nil {
		t.Error("new iter failed")
		return
	}
	defer iter.Release()
	count := 0
	for i := 0; iter.Next() != false; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		k := iter.Key()
		v := iter.Value()
		if bytes.Compare(key, k) != 0 || bytes.Compare(value, v) != 0 {
			t.Errorf("test iter failed [%s, %s] [%s, %s]", string(key), string(value), string(k), string(v))
			return
		}
		count++
	}
	if count != 100 {
		t.Errorf("test iter failed, count[%d], err[%v]", count, iter.Error())
		return
	}
}

func TestIter2(t *testing.T) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		t.Errorf("make dir failed, err[%v]", err)
		return
	}
	defer os.RemoveAll(path)
	file := filepath.Join(path, "test.db")
	store, applyId, err := NewBoltStore(file)
	if err != nil {
		t.Errorf("new store failed, err[%v]", err)
		return
	}
	if applyId != 0 {
		t.Errorf("test failed %d", applyId)
		return
	}
	defer store.Close()
	var buff [8]byte
	for i := 0; i < 100; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		err = store.Put(key, value, 0, ts.Timestamp{}, uint64(i))
		if err != nil {
			t.Errorf("put failed, err[%v]", err)
			return
		}
	}
	iter := store.NewIterator([]byte("key_"), []byte("kez"), ts.Timestamp{})
	if iter == nil {
		t.Error("new iter failed")
		return
	}
	defer iter.Release()
	// 新写入一批数据
	for i := 0; i < 100; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_n")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_n")...)
		value = append(value, buff[:]...)
		err = store.Put(key, value, 0, ts.Timestamp{}, uint64(i))
		if err != nil {
			t.Errorf("put failed, err[%v]", err)
			return
		}
	}

	count := 0
	for i := 0; iter.Next() != false; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		k := iter.Key()
		v := iter.Value()
		if bytes.Compare(key, k) != 0 || bytes.Compare(value, v) != 0 {
			t.Errorf("test iter failed [%s, %s] [%s, %s]", string(key), string(value), string(k), string(v))
			return
		}
		count++
	}
	if count != 100 {
		t.Errorf("test iter failed, count[%d], err[%v]", count, iter.Error())
		return
	}
}

func TestIter3(t *testing.T) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		t.Errorf("make dir failed, err[%v]", err)
		return
	}
	defer os.RemoveAll(path)
	file := filepath.Join(path, "test.db")
	store, applyId, err := NewBoltStore(file)
	if err != nil {
		t.Errorf("new store failed, err[%v]", err)
		return
	}
	if applyId != 0 {
		t.Errorf("test failed %d", applyId)
		return
	}
	defer store.Close()
	var buff [8]byte
	for i := 0; i < 100; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		err = store.Put(key, value, 0, ts.Timestamp{}, uint64(i))
		if err != nil {
			t.Errorf("put failed, err[%v]", err)
			return
		}
	}
	iter := store.NewIterator([]byte("key_"), []byte("kez"), ts.Timestamp{})
	if iter == nil {
		t.Error("new iter failed")
		return
	}
	defer iter.Release()

	// 新写入一批数据
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			var key, value []byte
			binary.BigEndian.PutUint64(buff[:], uint64(i))
			key = append(key, []byte("key_n")...)
			key = append(key, buff[:]...)
			value = append(value, []byte("value_n")...)
			value = append(value, buff[:]...)
			err = store.Put(key, value, 0, ts.Timestamp{}, uint64(i))
			if err != nil {
				t.Errorf("put failed, err[%v]", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	count := 0
	for i := 0; iter.Next() != false; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		k := iter.Key()
		v := iter.Value()
		if bytes.Compare(key, k) != 0 || bytes.Compare(value, v) != 0 {
			t.Errorf("test iter failed [%s, %s] [%s, %s]", string(key), string(value), string(k), string(v))
			return
		}
		count++
	}
	if count != 100 {
		t.Errorf("test iter failed, count[%d], err[%v]", count, iter.Error())
		return
	}
	wg.Wait()
}

func TestIter4(t *testing.T) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		t.Errorf("make dir failed, err[%v]", err)
		return
	}
	defer os.RemoveAll(path)
	file := filepath.Join(path, "test.db")
	store, applyId, err := NewBoltStore(file)
	if err != nil {
		t.Errorf("new store failed, err[%v]", err)
		return
	}
	if applyId != 0 {
		t.Errorf("test failed %d", applyId)
		return
	}
	defer store.Close()
	var buff [8]byte
	for i := 0; i < 100; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		err = store.Put(key, value, 0, ts.Timestamp{}, uint64(i))
		if err != nil {
			t.Errorf("put failed, err[%v]", err)
			return
		}
	}
	iter := store.NewIterator([]byte("key_"), []byte("kez"), ts.Timestamp{})
	if iter == nil {
		t.Error("new iter failed")
		return
	}
	defer iter.Release()

	if iter.Key() != nil {
		t.Error("iter key failed")
		return
	}
	count := 0
	for iter.Next() {
		count++
	}
	if count != 100 {
		t.Errorf("test iter failed, count[%d], err[%v]", count, iter.Error())
		return
	}
}

func TestSnap(t *testing.T) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		t.Errorf("make dir failed, err[%v]", err)
		return
	}
	defer os.RemoveAll(path)
	file := filepath.Join(path, "test.db")
	store, applyId, err := NewBoltStore(file)
	if err != nil {
		t.Errorf("new store failed, err[%v]", err)
		return
	}
	if applyId != 0 {
		t.Errorf("test failed %d", applyId)
		return
	}
	defer store.Close()
	var buff [8]byte
	for i := 0; i < 100; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		err = store.Put(key, value, 0, ts.Timestamp{}, uint64(i))
		if err != nil {
			t.Errorf("put failed, err[%v]", err)
			return
		}
	}
	snap, err := store.GetSnapshot()
	if err != nil {
		t.Errorf("test get snap failed, err[%v]", err)
		return
	}
	defer snap.Release()
	if snap.ApplyIndex() != 99 {
		t.Error("test failed")
		return
	}
	var key, value []byte
	binary.BigEndian.PutUint64(buff[:], uint64(3))
	key = append(key, []byte("key_")...)
	key = append(key, buff[:]...)
	value = append(value, []byte("value_")...)
	value = append(value, buff[:]...)
	v, err := snap.Get(key)
	if err != nil {
		t.Errorf("snap get key failed, err[%v]", err)
		return
	}
	if bytes.Compare(value, v) != 0 {
		t.Error("test snap failed")
		return
	}

	v, err = snap.Get([]byte("test"))
	if err != nil || v != nil {
		t.Errorf("snap get key failed, err[%v]", err)
		return
	}

	var start, end []byte
	binary.BigEndian.PutUint64(buff[:], uint64(3))
	start = append(start, []byte("key_")...)
	start = append(start, buff[:]...)
	binary.BigEndian.PutUint64(buff[:], uint64(89))
	end = append(end, []byte("key_")...)
	end = append(end, buff[:]...)
	iter := snap.NewIterator(start, end)
	if iter == nil {
		t.Error("new iter failed")
		return
	}
	defer iter.Release()
	count := 0
	for i := 3; iter.Next() != false; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		k := iter.Key()
		v := iter.Value()
		if bytes.Compare(key, k) != 0 || bytes.Compare(value, v) != 0 {
			t.Errorf("test iter failed [%s, %s] [%s, %s]", string(key), string(value), string(k), string(v))
			return
		}
		count++
	}
	if count != (89 - 3) {
		t.Errorf("test iter failed, count[%d], err[%v]", count, iter.Error())
		return
	}
}

func TestSnapIter(t *testing.T) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		t.Errorf("make dir failed, err[%v]", err)
		return
	}
	defer os.RemoveAll(path)
	file := filepath.Join(path, "test.db")
	store, applyId, err := NewBoltStore(file)
	if err != nil {
		t.Errorf("new store failed, err[%v]", err)
		return
	}
	if applyId != 0 {
		t.Errorf("test failed %d", applyId)
		return
	}
	defer store.Close()
	var buff [8]byte
	for i := 0; i < 100; i++ {
		var key, value []byte
		binary.BigEndian.PutUint64(buff[:], uint64(i))
		key = append(key, []byte("key_")...)
		key = append(key, buff[:]...)
		value = append(value, []byte("value_")...)
		value = append(value, buff[:]...)
		err = store.Put(key, value, 0, ts.Timestamp{}, uint64(i))
		if err != nil {
			t.Errorf("put failed, err[%v]", err)
			return
		}
	}
	snap, err := store.GetSnapshot()
	if err != nil {
		t.Errorf("test get snap failed, err[%v]", err)
		return
	}
	defer snap.Release()
	if snap.ApplyIndex() != 99 {
		t.Error("test failed")
		return
	}

	var start, end []byte
	binary.BigEndian.PutUint64(buff[:], uint64(3))
	start = append(start, []byte("key_")...)
	start = append(start, buff[:]...)
	binary.BigEndian.PutUint64(buff[:], uint64(89))
	end = append(end, []byte("key_")...)
	end = append(end, buff[:]...)
	iter := snap.NewIterator(start, end)
	if iter == nil {
		t.Error("new iter failed")
		return
	}
	defer iter.Release()
	if iter.Key() != nil {
		t.Error("iter key failed")
		return
	}
	count := 0
	for iter.Next() {
		count++
	}
	if count != 86 {
		t.Errorf("test iter failed, count[%d], err[%v]", count, iter.Error())
		return
	}
}

func TestBatch(t *testing.T) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		t.Errorf("make dir failed, err[%v]", err)
		return
	}
	defer os.RemoveAll(path)
	file := filepath.Join(path, "test.db")
	store, applyId, err := NewBoltStore(file)
	if err != nil {
		t.Errorf("new store failed, err[%v]", err)
		return
	}
	if applyId != 0 {
		t.Errorf("test failed %d", applyId)
		return
	}
	defer store.Close()
	batch := store.NewWriteBatch()
	if batch == nil {
		t.Error("new batch failed")
		return
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		batch.Put(key, value, 0, ts.Timestamp{}, uint64(i))
	}
	err = batch.Commit()
	if err != nil {
		t.Errorf("batch commit failed, err[%v]", err)
		return
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		eValue := []byte(fmt.Sprintf("value_%d", i))
		value, err := store.Get(key, ts.Timestamp{})
		if err != nil {
			t.Errorf("get failed, err[%v]", err)
			return
		}
		if bytes.Compare(eValue, value) != 0 {
			t.Error("get failed")
		}
	}
	snap, err := store.GetSnapshot()
	if err != nil {
		t.Errorf("get snap failed, err[%v]", err)
		return
	}
	if snap.ApplyIndex() != 99 {
		t.Error("test failed")
		return
	}
	snap.Release()

	batch = store.NewWriteBatch()
	if batch == nil {
		t.Error("new batch failed")
		return
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		batch.Delete(key, ts.Timestamp{}, uint64(i))
	}
	err = batch.Commit()
	if err != nil {
		t.Errorf("batch commit failed, err[%v]", err)
		return
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		err := store.Delete(key, ts.Timestamp{}, uint64(i))
		if err != nil {
			t.Errorf("get failed, err[%v]", err)
			return
		}
		value, err := store.Get(key, ts.Timestamp{})
		if err != nil {
			if err.Error() != errors.ErrNotFound.Error() {
				t.Errorf("get failed, err[%v]", err)
				return
			}
		} else if len(value) != 0 {
			t.Error("test delete failed")
			return
		}
	}
}
