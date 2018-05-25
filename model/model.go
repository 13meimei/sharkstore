package model

import "errors"

var (
	ErrStoreKeyNotFound = errors.New("key not found")
)

type KVStore interface {
	Open() error
	Close() error

	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error

	// scan
	NewIterator(startKey, endKey []byte) Iterator

	// Batch
	NewWriteBatch() WriteBatch

	// Get Snapshot
	GetSnapshot() (Snapshot, error)
	
	Scan(prefix []byte) Iterator

	Destroy()

	// Size KV Pair Size，不压缩
	Size() int64

	// DiskUsage fdb table file size
	DiskUsage() uint64

	// FindMiddleKey 查找中间Key
	FindMiddleKey() ([]byte, error)

	// split store to two store
	// paths has two path
	Split(splitKey []byte, paths []string)([]KVStore, error)
}

type Iterator interface {
	// return false if over
	Next() bool

	Key() []byte
	Value() []byte

	Error() error
	Release()
}

type Snapshot interface {
	NewIterator(startKey, endKey []byte) Iterator
	Get(key []byte) ([]byte, error)
	Release()
}

type WriteBatch interface {
	Put(key, value []byte)
	Delete(key []byte)
	Commit() error
}

type MasterConfig struct {
	Id    uint64
	Peers map[uint64]*NodeInfo
}

type NodeInfo struct {
	Ip     string
	Port   int
	Id     uint64
	Status int
}

type DataNode struct {
	NodeInfo
}

