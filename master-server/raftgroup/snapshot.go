package raftgroup

import (
	"errors"
	"io"

	"master-server/engine/model"
	raftproto "master-server/raft/proto"

	"github.com/gogo/protobuf/proto"
	"model/pkg/ms_raftcmdpb"
)

var (
	errCorruptData = errors.New("corrupt data")
)

type RaftSnapshot struct {
	snap       model.Snapshot
	applyIndex uint64
	iter       model.Iterator
}

func NewRaftSnapshot(snap model.Snapshot, applyIndex uint64, beginKey, endKey []byte) *RaftSnapshot {
	s := &RaftSnapshot{
		snap:       snap,
		applyIndex: applyIndex,
		iter:       snap.NewIterator(beginKey, endKey),
	}

	return s
}

func (s *RaftSnapshot) Next() ([]byte, error) {
	err := s.iter.Error()
	if err != nil {
		return nil, err
	}

	hasNext := s.iter.Next()
	if !hasNext {
		return nil, io.EOF
	}
	kvPair := &ms_raftcmdpb.RaftKvPair{
		Key:      s.iter.Key(),
		Value:    s.iter.Value(),
		ApplyIndex: s.ApplyIndex(),
	}

	return proto.Marshal(kvPair)
}

func (s *RaftSnapshot) ApplyIndex() uint64 {
	return s.applyIndex
}

func (s *RaftSnapshot) Close() {
	s.iter.Release()
	s.snap.Release()
}

type SnapshotKVIterator struct {
	rawIter raftproto.SnapIterator
}

func NewSnapshotKVIterator(rawIter raftproto.SnapIterator) *SnapshotKVIterator {
	return &SnapshotKVIterator{
		rawIter: rawIter,
	}
}

func (i *SnapshotKVIterator) Next() (kvPair *ms_raftcmdpb.RaftKvPair, err error) {
	var data []byte
	data, err = i.rawIter.Next()
	if err != nil {
		return
	}
	kvPair = &ms_raftcmdpb.RaftKvPair{}
	err = proto.Unmarshal(data, kvPair)
	if err != nil {
		return nil, err
	}
	return kvPair, nil
}
