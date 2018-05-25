package goleveldb

import (
	"github.com/syndtr/goleveldb/leveldb"

	"proxy/store/localstore/engine"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDBDriver struct {
	db    *leveldb.DB
}

func NewLevelDBDriver(path string) (engine.Driver, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &LevelDBDriver{db}, nil
}

func (ld *LevelDBDriver)Get(key []byte) (value []byte, err error) {
	if ld == nil {
		return nil, nil
	}
	value, err = ld.db.Get(key, nil)
	if err != nil && err == leveldb.ErrNotFound {
		err = engine.ErrNotFound
		return
	}
	return
}

func (ld *LevelDBDriver)Put(key []byte, value []byte) error {
	if ld == nil {
		return nil
	}
	return ld.db.Put(key, value, nil)
}

func (ld *LevelDBDriver)Delete(key []byte) error {
	if ld == nil {
		return nil
	}
	return ld.db.Delete(key, nil)
}

func (ld *LevelDBDriver)Close() error {
	if ld == nil {
		return nil
	}
	return ld.db.Close()
}

func (ld *LevelDBDriver)NewIterator(startKey, endKey []byte) engine.Iterator {
	if ld == nil {
		return nil
	}
	return &LevelDBIter{ld.db.NewIterator(&util.Range{Start: startKey, Limit: endKey}, nil)}
}

// 批量写入，提交时保证batch里的修改同时对外可见
func (ld *LevelDBDriver)NewBatch() engine.Batch {
	return &LevelDBBatch{db: ld.db, batch: &leveldb.Batch{}}
}

func (ld *LevelDBDriver)GetSnapshot() (engine.Snapshot, error) {
	snap, err := ld.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &LevelDBSnapshot{snap}, nil
}

type LevelDBIter struct {
	iter   iterator.Iterator
}

func (i *LevelDBIter)Next() bool {
	return i.iter.Next()
}

func (i *LevelDBIter)Key() []byte {
	return i.iter.Key()
}

func (i *LevelDBIter)Value() []byte {
	return i.iter.Value()
}

func (i *LevelDBIter)Error() error {
	return i.iter.Error()
}

// Release iterator使用完需要释放
func (i *LevelDBIter)Release() {
	i.iter.Release()
}

type LevelDBSnapshot struct {
	snap   *leveldb.Snapshot
}

func (s *LevelDBSnapshot) NewIterator(startKey, endKey []byte) engine.Iterator {
	return &LevelDBIter{s.snap.NewIterator(&util.Range{Start: startKey, Limit: endKey}, nil)}
}

func (s *LevelDBSnapshot) Get(key []byte) ([]byte, error) {
	return s.snap.Get(key, nil)
}

// Release snapshot使用完需要释放
func (s *LevelDBSnapshot) Release() {
	s.snap.Release()
}

type LevelDBBatch struct {
	db    *leveldb.DB
	batch *leveldb.Batch
}

func (b *LevelDBBatch)Put(key []byte, value []byte) {
	b.batch.Put(key, value)
}

func (b *LevelDBBatch)Delete(key []byte) {
	b.batch.Delete(key)
}

func (b *LevelDBBatch)Commit() error {
	return b.db.Write(b.batch, nil)
}

