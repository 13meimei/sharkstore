package model

import ts "model/pkg/timestamp"

// Store store
type Store interface {
	Get(key []byte, timestamp ts.Timestamp) (value []byte, err error)
	Put(key []byte, value []byte, expireAt int64, timestamp ts.Timestamp, raftIndex uint64) error
	Delete(key []byte, timestamp ts.Timestamp, raftIndex uint64) error
	Close() error

	NewIterator(startKey, endKey []byte, timestamp ts.Timestamp) Iterator

	// 批量写入，提交时保证batch里的修改同时对外可见
	NewWriteBatch() WriteBatch

	GetSnapshot() (Snapshot, error)

	// Applied return current applied raft index(已持久化的)
	Applied() uint64
}

// Iterator iterator
type Iterator interface {
	// return false if over or error
	Next() bool

	Key() []byte
	Value() []byte

	Error() error

	// Release iterator使用完需要释放
	Release()
}

// Snapshot snapshot
type Snapshot interface {
	NewIterator(startKey, endKey []byte) Iterator
	Get(key []byte) ([]byte, error)
	// apply index
	ApplyIndex() uint64

	// Release snapshot使用完需要释放
	Release()
}

// WriteBatch write batch
type WriteBatch interface {
	Put(key []byte, value []byte, expireAt int64, timestamp ts.Timestamp, raftIndex uint64)
	Delete(key []byte, timestamp ts.Timestamp, raftIndex uint64)

	Commit() error
}