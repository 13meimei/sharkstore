package boltstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
	"util"

	"github.com/boltdb/bolt"
	"master-server/engine/model"
)

var RAFT_APPLY_ID []byte = []byte("#raft_apply_id")

var RAFT_BUCKET []byte = []byte("RaftBucket")
var DB_BUCKET []byte = []byte("DbBucket")

// Store store
type BoltStore struct {
	db *bolt.DB
}

func NewBoltStore(path string) (model.Store, uint64, error) {
	db, err := bolt.Open(path, 0664, &bolt.Options{Timeout: 1 * time.Second, InitialMmapSize: 2 * util.GB})
	if err != nil {
		return nil, 0, err
	}
	db.NoSync = true
	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(RAFT_BUCKET)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		_, err = tx.CreateBucketIfNotExists(DB_BUCKET)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	// read applyID
	var value []byte
	var applyId uint64
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(RAFT_BUCKET)
		v := b.Get(RAFT_APPLY_ID)
		value = cloneBytes(v)
		return nil
	})
	if value == nil {
		applyId = 0
	} else {
		applyId = binary.BigEndian.Uint64(value)
	}
	store := &BoltStore{db: db}
	return store, applyId, nil
}

func (bs *BoltStore) Get(key []byte) (value []byte, err error) {
	if bs == nil {
		return nil, nil
	}
	err = bs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(DB_BUCKET)
		v := b.Get(key)
		//if v == nil {
		//	return errors.ErrNotFound
		//}
		if v != nil {
			value = cloneBytes(v)
		}
		return nil
	})
	return
}

func (bs *BoltStore) Put(key []byte, value []byte, expireAt int64, raftIndex uint64) error {
	if bs == nil {
		return nil
	}
	return bs.db.Update(func(tx *bolt.Tx) error {
		var buff [8]byte
		b := tx.Bucket(DB_BUCKET)
		err := b.Put(key, value)
		if err != nil {
			return err
		}
		// store raft apply ID
		r := tx.Bucket(RAFT_BUCKET)
		binary.BigEndian.PutUint64(buff[:], raftIndex)
		err = r.Put(RAFT_APPLY_ID, buff[:])
		if err != nil {
			return err
		}
		return nil
	})
}

func (bs *BoltStore) Delete(key []byte, raftIndex uint64) error {
	if bs == nil {
		return nil
	}
	return bs.db.Update(func(tx *bolt.Tx) error {
		var buff [8]byte
		b := tx.Bucket(DB_BUCKET)
		err := b.Delete(key)
		if err != nil {
			return err
		}
		// store raft apply ID
		r := tx.Bucket(RAFT_BUCKET)
		binary.BigEndian.PutUint64(buff[:], raftIndex)
		err = r.Put(RAFT_APPLY_ID, buff[:])
		if err != nil {
			return err
		}
		return nil
	})
}

func (bs *BoltStore) Close() error {
	if bs == nil {
		return nil
	}
	bs.db.Sync()
	return bs.db.Close()
}

func (bs *BoltStore) NewIterator(startKey, endKey []byte) model.Iterator {
	iter, err := NewBoltIterator(bs.db, startKey, endKey)
	if err != nil {
		return nil
	}
	return iter
}

// 批量写入，提交时保证batch里的修改同时对外可见
func (bs *BoltStore) NewWriteBatch() model.WriteBatch {
	batch, err := NewBoltWriteBatch(bs.db)
	if err != nil {
		return nil
	}
	return batch
}

func (bs *BoltStore) GetSnapshot() (model.Snapshot, error) {
	return NewBoltSnapshot(bs.db)
}

// Applied return current applied raft index(已持久化的)
func (bs *BoltStore) Applied() uint64 {
	var value []byte
	var applyId uint64
	err := bs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(RAFT_BUCKET)
		v := b.Get(RAFT_APPLY_ID)
		if v != nil {
			value = cloneBytes(v)
		}
		return nil
	})
	if err != nil || value == nil {
		applyId = 0
	} else {
		applyId = binary.BigEndian.Uint64(value)
	}
	return applyId
}

type KvPair struct {
	Key   []byte
	Value []byte
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

func NewBoltIterator(db *bolt.DB, startKey, endKey []byte) (*BoltIterator, error) {
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
func (iter *BoltIterator) Next() bool {
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
				Key:   k,
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

func (iter *BoltIterator) Key() []byte {
	if iter == nil {
		return nil
	}

	if iter.kvPair != nil {
		return iter.kvPair.Key
	}
	return nil
}

func (iter *BoltIterator) Value() []byte {
	if iter == nil {
		return nil
	}

	if iter.kvPair != nil {
		return iter.kvPair.Value
	}
	return nil
}

func (iter *BoltIterator) Error() error {
	if iter == nil {
		return nil
	}
	return iter.lastErr
}

// Release iterator使用完需要释放
func (iter *BoltIterator) Release() {
	if iter == nil {
		return
	}
	iter.tx.Rollback()
}

// Snapshot snapshot
type BoltSnapshot struct {
	applyID uint64
	db      *bolt.DB
	tx      *bolt.Tx
	b       *bolt.Bucket
}

func NewBoltSnapshot(db *bolt.DB) (*BoltSnapshot, error) {
	var applyId uint64
	tx, err := db.Begin(false)
	if err != nil {
		return nil, err
	}
	b := tx.Bucket(DB_BUCKET)
	r := tx.Bucket(RAFT_BUCKET)
	v := r.Get(RAFT_APPLY_ID)
	if len(v) == 0 {
		applyId = 0
	} else {
		applyId = binary.BigEndian.Uint64(v)
	}

	return &BoltSnapshot{db: db, tx: tx, b: b, applyID: applyId}, nil
}

type SnapIterTx struct {
	tx *bolt.Tx
}

func (st *SnapIterTx) Bucket(name []byte) *bolt.Bucket {
	return st.tx.Bucket(name)
}

func (st *SnapIterTx) Rollback() error {
	return nil
}

func (bs *BoltSnapshot) NewIterator(startKey, endKey []byte) model.Iterator {
	c := bs.b.Cursor()
	_start := make([]byte, len(startKey))
	_limit := make([]byte, len(endKey))
	copy(_start, startKey)
	copy(_limit, endKey)
	return &BoltIterator{start: _start, limit: _limit, db: bs.db, tx: &SnapIterTx{bs.tx}, iter: c, first: true}
}

func (bs *BoltSnapshot) Get(key []byte) ([]byte, error) {
	if bs == nil {
		return nil, nil
	}
	var value []byte
	v := bs.b.Get(key)
	if v != nil {
		value = cloneBytes(v)
	}
	return value, nil
}

// apply index
func (bs *BoltSnapshot) ApplyIndex() uint64 {
	if bs == nil {
		return 0
	}
	return bs.applyID
}

// Release snapshot使用完需要释放
func (bs *BoltSnapshot) Release() {
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
type BoltWriteBatch struct {
	db *bolt.DB

	writes    []write
	raftIndex uint64
	err       error
}

func NewBoltWriteBatch(db *bolt.DB) (*BoltWriteBatch, error) {
	return &BoltWriteBatch{db: db}, nil
}

func (bb *BoltWriteBatch) Put(key []byte, value []byte, expireAt int64, raftIndex uint64) {
	w := write{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	}
	bb.writes = append(bb.writes, w)
	bb.raftIndex = raftIndex
}

func (bb *BoltWriteBatch) Delete(key []byte, raftIndex uint64) {
	if bb == nil {
		return
	}
	w := write{
		key:      append([]byte(nil), key...),
		isDelete: true,
	}
	bb.writes = append(bb.writes, w)
	bb.raftIndex = raftIndex
}

func (bb *BoltWriteBatch) Commit() error {
	if bb == nil {
		return nil
	}
	return bb.db.Update(func(tx *bolt.Tx) error {
		var buff [8]byte
		r := tx.Bucket(RAFT_BUCKET)
		binary.BigEndian.PutUint64(buff[:], bb.raftIndex)
		err := r.Put(RAFT_APPLY_ID, buff[:])
		if err != nil {
			return err
		}
		b := tx.Bucket(DB_BUCKET)
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
