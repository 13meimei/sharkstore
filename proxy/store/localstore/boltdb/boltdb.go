package boltdb

import (
	"time"
	"fmt"
	"bytes"

	"util"
	"github.com/boltdb/bolt"
	"proxy/store/localstore/engine"
)

var DB_BUCKET []byte = []byte("DbBucket")

// Store store
type BoltDriver struct {
	db     *bolt.DB
}

func NewBoltDriver(path string) (engine.Driver, error) {
	db, err := bolt.Open(path, 0664, &bolt.Options{Timeout: 1 * time.Second, InitialMmapSize: 2*util.GB})
	if err != nil {
		return nil, err
	}
	db.NoSync = true
	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(DB_BUCKET)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &BoltDriver{db: db}, nil
}

func (bs *BoltDriver)Get(key []byte) (value []byte, err error) {
	if bs == nil {
		return nil, nil
	}
	err = bs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(DB_BUCKET)
		v := b.Get(key)
		if v == nil {
			return engine.ErrNotFound
		}
		value = cloneBytes(v)
		return nil
	})
	return
}

func (bs *BoltDriver)Put(key []byte, value []byte) error {
	if bs == nil {
		return nil
	}
	return bs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(DB_BUCKET)
		err := b.Put(key, value)
		if err != nil {
			return err
		}
		return nil
	})
}

func (bs *BoltDriver)Delete(key []byte) error {
	if bs == nil {
		return nil
	}
	return bs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(DB_BUCKET)
		err := b.Delete(key)
		if err != nil {
			return err
		}
		return nil
	})
}

func (bs *BoltDriver)Close() error {
	if bs == nil {
		return nil
	}
	bs.db.Sync()
	return bs.db.Close()
}

func (bs *BoltDriver)NewIterator(startKey, endKey []byte) engine.Iterator {
	iter, err := NewBoltIterator(bs.db, startKey, endKey)
	if err != nil {
		return nil
	}
	return iter
}

// 批量写入，提交时保证batch里的修改同时对外可见
func (bs *BoltDriver)NewBatch() engine.Batch {
	batch, err := NewBoltBatch(bs.db)
	if err != nil {
		return nil
	}
	return batch
}

func (bs *BoltDriver)GetSnapshot() (engine.Snapshot, error) {
	return NewBoltSnapshot(bs.db)
}

type KvPair struct {
	Key     []byte
	Value   []byte
}

type Tx interface {
	Bucket(name []byte) *bolt.Bucket
	Rollback() error
}

// Iterator iterator
type BoltIterator struct {
	start   []byte
	limit   []byte
	kvPair  *KvPair
	first   bool
	lastErr error
	db      *bolt.DB
	tx      Tx
	iter    *bolt.Cursor
}

func NewBoltIterator(db *bolt.DB, startKey, endKey []byte) (*BoltIterator, error){
	tx, err := db.Begin(false)
	if err != nil {
		return nil, err
	}
	b := tx.Bucket(DB_BUCKET)
	c := b.Cursor()
	_start := make([]byte, len(startKey))
	_limit := make([]byte, len(endKey))
	copy(_start, startKey)
	copy(_limit, endKey)
	iter := &BoltIterator{start: _start, limit: _limit, db: db, tx: tx, iter: c, first: true}
	return iter, nil
}

// return false if over or error
func (iter *BoltIterator)Next() bool {
	if iter == nil {
		return false
	}
	var k, v []byte
	if iter.first {
		k, v = iter.iter.Seek(iter.start)
		iter.first = false
	} else {
		k, v = iter.iter.Next()
	}
	if k != nil && bytes.Compare(k, iter.limit) < 0 {
		// Please note that values returned from Get() are only valid while the transaction is open.
		// If you need to use a value outside of the transaction then you must use copy() to copy it
		// to another byte slice.
		if iter.kvPair == nil {
			iter.kvPair = &KvPair{
				Key: k,
				Value: v,
			}
		} else {
			iter.kvPair.Key = k
			iter.kvPair.Value = v
		}

		return true
	} else {
		return false
	}
}

func (iter *BoltIterator)Key() []byte {
	if iter == nil {
		return nil
	}

	if iter.kvPair != nil {
		return iter.kvPair.Key
	}
	return nil
}

func (iter *BoltIterator)Value() []byte {
	if iter == nil {
		return nil
	}

	if iter.kvPair != nil {
		return iter.kvPair.Value
	}
	return nil
}

func (iter *BoltIterator)Error() error {
	if iter == nil {
		return nil
	}
	return iter.lastErr
}

// Release iterator使用完需要释放
func (iter *BoltIterator)Release() {
	if iter == nil {
		return
	}
	iter.tx.Rollback()
}

// Snapshot snapshot
type BoltSnapshot struct {
	db     *bolt.DB
	tx     *bolt.Tx
	b      *bolt.Bucket
}

func NewBoltSnapshot(db *bolt.DB) (*BoltSnapshot, error) {
	tx, err := db.Begin(false)
	if err != nil {
		return nil, err
	}
	b := tx.Bucket(DB_BUCKET)

	return &BoltSnapshot{db: db, tx: tx, b: b}, nil
}

type SnapIterTx struct {
	tx  *bolt.Tx
}

func (st *SnapIterTx)Bucket(name []byte) *bolt.Bucket {
	return st.tx.Bucket(name)
}

func (st *SnapIterTx)Rollback() error {
	return nil
}

func (bs *BoltSnapshot)NewIterator(startKey, endKey []byte) engine.Iterator {
	c := bs.b.Cursor()
	_start := make([]byte, len(startKey))
	_limit := make([]byte, len(endKey))
	copy(_start, startKey)
	copy(_limit, endKey)
	return &BoltIterator{start: _start, limit: _limit, db: bs.db, tx: &SnapIterTx{bs.tx}, iter: c, first: true}
}

func (bs *BoltSnapshot)Get(key []byte) ([]byte, error) {
	if bs == nil {
		return nil, nil
	}
	v := cloneBytes(bs.b.Get(key))
	return v, nil
}


// Release snapshot使用完需要释放
func (bs *BoltSnapshot)Release() {
	if bs == nil {
		return
	}
	bs.tx.Rollback()
}

type write struct {
	key      []byte
	value    []byte
	isDelete bool
}

// WriteBatch write batch
type BoltBatch struct {
	db     *bolt.DB

	writes []write
	err    error
}

func NewBoltBatch(db *bolt.DB) (*BoltBatch, error){
	return &BoltBatch{db: db}, nil
}

func (bb *BoltBatch)Put(key []byte, value []byte) {
	w := write{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	}
	bb.writes = append(bb.writes, w)
}

func (bb *BoltBatch)Delete(key []byte) {
	if bb == nil {
		return
	}
	w := write{
		key:   append([]byte(nil), key...),
		isDelete: true,
	}
	bb.writes = append(bb.writes, w)
}

func (bb *BoltBatch)Commit() error {
	if bb == nil {
		return nil
	}
	return bb.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(DB_BUCKET)
		var err error
		for _, w := range bb.writes {
			if !w.isDelete {
				err = b.Put(w.key, w.value)
			} else {
				err = b.Delete(w.key)
			}

			if err != nil {
				return err
			}
		}
		return nil
	})
}

func cloneBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}
