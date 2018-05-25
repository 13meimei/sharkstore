package storage

import (
	"bytes"
	"errors"
	"fmt"

	"encoding/binary"

	"model"
	"master-server/raft/logger"
	"master-server/raft/proto"
)

type PersistStorage struct {
	RangeID    uint64 // the rangeID
	engine     model.KVStore
	truncIndex uint64
	truncTerm  uint64
	lastIndex  uint64
}

func NewPersistStorage(rangeID uint64, store model.KVStore) (*PersistStorage, error) {
	s := &PersistStorage{
		RangeID: rangeID,
		engine:  store,
	}

	// try to reload lastIndex
	lastIndex, err := s.loadLastIndex()
	if err != nil {
		return nil, err
	}
	s.lastIndex = lastIndex

	// try to reload trunc meta
	meta, err := s.loadSnapshotMeta()
	if err != nil {
		return nil, err
	}
	s.truncIndex = meta.Index
	s.truncTerm = meta.Term

	return s, nil
}

// lastIndex
func (s *PersistStorage) loadLastIndex() (index uint64, err error) {
	var key []byte
	key = model.MakeRaftLastIndexKey(s.RangeID)

	var data []byte
	data, err = s.engine.Get(key)
	if err != nil {
		if err == model.ErrStoreKeyNotFound {
			err = nil
		}
		return
	}
	index = binary.BigEndian.Uint64(data)
	return
}

func (s *PersistStorage) storeLastIndex(index uint64) error {
	s.lastIndex = index
	key := model.MakeRaftLastIndexKey(s.RangeID)
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, index)
	return s.engine.Put(key, value)
}

func (s *PersistStorage) InitialState() (hs proto.HardState, err error) {
	var key []byte
	key = model.MakeRaftHardStateKey(s.RangeID)

	var data []byte
	data, err = s.engine.Get(key)
	if err != nil {
		if err == model.ErrStoreKeyNotFound {
			err = nil
		}
		return
	}

	if uint64(len(data)) != hs.Size() {
		err = fmt.Errorf("unexpcetd hardstate size: %d", len(data))
		return
	}
	hs.Decode(data)
	return
}

func (s *PersistStorage) Entries(lo, hi uint64, maxSize uint64) ([]*proto.Entry, bool, error) {
	if lo <= s.truncIndex {
		return nil, true, nil
	}
	if hi > s.lastIndex+1 {
		return nil, false, fmt.Errorf("[PersistStorage->Entries]entries's hi(%d) is out of bound lastindex(%d)", hi, s.lastIndex)
	}

	if hi-lo < 0 {
		return []*proto.Entry{}, false, nil
	}

	if maxSize == 0 {
		maxSize = 1 //  returns at least one entry if any.
	}
	if hi-lo < maxSize {
		maxSize = hi - lo
	}

	startKey := model.MakeRaftLogEntryKey(s.RangeID, lo)
	endKey := model.MakeRaftLogEntryKey(s.RangeID, hi)
	iter := s.engine.NewIterator(startKey, endKey)
	defer iter.Release()
	entries := make([]*proto.Entry, 0, maxSize)
	for i := uint64(0); i < maxSize; i++ {
		if !iter.Next() {
			if iter.Error() != nil {
				return nil, false, iter.Error()
			} else {
				return entries, false, nil
			}
		}

		index := i + lo
		key := model.MakeRaftLogEntryKey(s.RangeID, index)
		data, err := s.engine.Get(key)
		if err != nil {
			return nil, false, err
		}
		entry := &proto.Entry{}
		entry.Decode(data)
		if entry.Index != index {
			return nil, false, fmt.Errorf("[PersistStorage->Entries]mismatch entry index. expected: %d, store actual: %d", index, entry.Index)
		}
		entries = append(entries, entry)
	}
	return entries, false, nil
}

func (s *PersistStorage) Term(index uint64) (term uint64, isCompact bool, err error) {
	switch {
	case index < s.truncIndex:
		return 0, true, nil
	case index == s.truncIndex:
		return s.truncTerm, false, nil
	default:
		entries, isCompact, err := s.Entries(index, index+1, 1)
		if err != nil || isCompact {
			return 0, isCompact, err
		}
		if len(entries) == 0 {
			return 0, false, fmt.Errorf("[PersistStorage->Entries] no such entry. index: %d", index)
		}
		return entries[0].Term, false, nil
	}
}

func (s *PersistStorage) FirstIndex() (uint64, error) {
	return s.truncIndex + 1, nil
}

func (s *PersistStorage) LastIndex() (uint64, error) {
	return s.lastIndex, nil
}

func (s *PersistStorage) StoreEntries(entries []*proto.Entry) error {
	newLen := len(entries)
	if newLen == 0 {
		return nil
	}

	// check skip
	if entries[0].Index > s.lastIndex+1 {
		logger.Error("missing log entry [last: %d, append at: %d]", s.lastIndex, entries[0].Index)
		return nil
	}

	for _, entry := range entries {
		if entry.Index <= s.truncIndex {
			continue //TODO:
			logger.Warn("log entry index to stroe bellow trunc [index: %d, trunc: %d]", entry.Index, s.truncIndex)
		}
		key := model.MakeRaftLogEntryKey(s.RangeID, entry.Index)
		buffer := bytes.NewBuffer(nil)
		err := entry.Encode(buffer)
		if err != nil {
			return err
		}

		err = s.engine.Put(key, buffer.Bytes())
		if err != nil {
			return err
		}
	}

	newLastIndex := entries[newLen-1].Index
	if newLastIndex < s.lastIndex {
		for i := newLastIndex + 1; i <= s.lastIndex; i++ {
			key := model.MakeRaftLogEntryKey(s.RangeID, i)
			err := s.engine.Delete(key)
			if err != nil {
				return err
			}
		}
	}
	if newLastIndex != s.lastIndex {
		return s.storeLastIndex(newLastIndex)
	}
	return nil
}

func (s *PersistStorage) StoreHardState(st proto.HardState) error {
	key := model.MakeRaftHardStateKey(s.RangeID)
	datas := make([]byte, st.Size())
	st.Encode(datas)
	return s.engine.Put(key, datas)
}

func (s *PersistStorage) Truncate(index uint64) error {
	if index == 0 || index <= s.truncIndex {
		return errors.New("requested index is unavailable due to compaction")
	}
	if index > s.lastIndex {
		return fmt.Errorf("compact %d is out of bound lastindex(%d)", index, s.lastIndex)
	}

	return s.truncateTo(index)
}

func (s *PersistStorage) truncateTo(index uint64) error {
	term, isCompact, err := s.Term(index)
	if err != nil {
		return err
	}
	if isCompact {
		return errors.New("requested index is unavailable due to compaction")
	}

	for i := s.truncIndex + 1; i <= index; i++ {
		key := model.MakeRaftLogEntryKey(s.RangeID, i)
		err := s.engine.Delete(key)
		if err != nil {
			return err
		}
	}

	return s.storeSnapshotMeta(proto.SnapshotMeta{Index: index, Term: term})
}

func (s *PersistStorage) loadSnapshotMeta() (meta proto.SnapshotMeta, err error) {
	var key []byte
	key = model.MakeRaftSnapshotMetaKey(s.RangeID)

	datas, err := s.engine.Get(key)
	if err != nil {
		if err == model.ErrStoreKeyNotFound {
			err = nil
		}
		return
	}

	meta.Decode(datas)
	return
}

func (s *PersistStorage) storeSnapshotMeta(meta proto.SnapshotMeta) error {
	s.truncIndex = meta.Index
	s.truncTerm = meta.Term

	key := model.MakeRaftSnapshotMetaKey(s.RangeID)

	buffer := bytes.NewBuffer(nil)
	err := meta.Encode(buffer)
	if err != nil {
		return err
	}
	err = s.engine.Put(key, buffer.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (s *PersistStorage) ApplySnapshot(meta proto.SnapshotMeta) error {
	// 清空logs
	for i := s.truncIndex + 1; i <= s.lastIndex; i++ {
		key := model.MakeRaftLogEntryKey(s.RangeID, i)
		err := s.engine.Delete(key)
		if err != nil {
			return err
		}
	}

	err := s.storeSnapshotMeta(meta)
	if err != nil {
		return err
	}

	if s.lastIndex != meta.Index {
		return s.storeLastIndex(meta.Index)
	}

	return nil
}

func (s *PersistStorage) Close() {
}

// ApplyIndex
func (s *PersistStorage) ApplyIndex() (index uint64, err error) {
	var key []byte
	key = model.MakeRaftApplyIndexKey(s.RangeID)
	if err != nil {
		return
	}

	var data []byte
	data, err = s.engine.Get(key)
	if err != nil {
		if err == model.ErrStoreKeyNotFound {
			err = nil
		}
		return
	}

	index = binary.BigEndian.Uint64(data)
	return
}

func (s *PersistStorage) SnapshotApplyIndex(snap model.Snapshot) (index uint64, err error) {
	var key []byte
	key = model.MakeRaftApplyIndexKey(s.RangeID)
	if err != nil {
		return
	}

	var data []byte
	data, err = snap.Get(key)
	if err != nil {
		if err == model.ErrStoreKeyNotFound {
			err = nil
		}
		return
	}

	index = binary.BigEndian.Uint64(data)
	return
}

func (s *PersistStorage) StoreApplyIndex(applyIndex uint64) error {
	key := model.MakeRaftApplyIndexKey(s.RangeID)
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, applyIndex)
	return s.engine.Put(key, value)
}

func (s *PersistStorage) StoreApplyIndexBatch(batch model.WriteBatch, applyIndex uint64) error {
	key := model.MakeRaftApplyIndexKey(s.RangeID)
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, applyIndex)
	batch.Put(key, value)
	return nil
}
