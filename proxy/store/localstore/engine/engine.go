package engine

// Driver
type Driver interface {
	Get(key []byte) (value []byte, err error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	Close() error

	NewIterator(startKey, endKey []byte) Iterator

	// 批量写入，提交时保证batch里的修改同时对外可见
	NewBatch() Batch

	GetSnapshot() (Snapshot, error)
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

	// Release snapshot使用完需要释放
	Release()
}

// WriteBatch write batch
type Batch interface {
	Put(key []byte, value []byte)
	Delete(key []byte)

	Commit() error
}
