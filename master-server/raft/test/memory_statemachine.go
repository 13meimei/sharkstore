package test

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"master-server/raft"
	"master-server/raft/proto"
)

var errNotExists = errors.New("Key not exists.")

type memoryStatemachine struct {
	sync.RWMutex
	id      uint64
	applied uint64
	raft    *raft.RaftServer
	data    map[string]string
}

func newMemoryStatemachine(id uint64, raft *raft.RaftServer) *memoryStatemachine {
	return &memoryStatemachine{
		id:   id,
		raft: raft,
		data: make(map[string]string),
	}
}

func (ms *memoryStatemachine) Apply(data []byte, index uint64) (interface{}, error) {
	ms.Lock()
	defer func() {
		ms.Unlock()
	}()

	var kv map[string]string
	if err := json.Unmarshal(data, &kv); err != nil {
		return nil, err
	}
	for k, v := range kv {
		ms.data[k] = v
	}
	return nil, nil
}

func (ms *memoryStatemachine) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	ms.Lock()
	defer func() {
		ms.Unlock()
	}()

	return nil, nil
}

func (ms *memoryStatemachine) Snapshot() (proto.Snapshot, error) {
	ms.RLock()
	defer ms.RUnlock()

	if data, err := json.Marshal(ms.data); err != nil {
		return nil, err
	} else {
		data = append(make([]byte, 8), data...)
		binary.BigEndian.PutUint64(data, ms.applied)
		return &memorySnapshot{
			applied: ms.applied,
			data:    data,
		}, nil
	}
}

func (ms *memoryStatemachine) ApplySnapshot(iter proto.SnapIterator) error {
	ms.Lock()
	defer ms.Unlock()

	var (
		data  []byte
		block []byte
		err   error
	)
	for err == nil {
		if block, err = iter.Next(); len(block) > 0 {
			data = append(data, block...)
		}
	}
	if err != nil && err != io.EOF {
		return err
	}

	ms.applied = binary.BigEndian.Uint64(data)
	if err = json.Unmarshal(data[8:], &ms.data); err != nil {
		return err
	}
	return nil
}

func (ms *memoryStatemachine) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

func (ms *memoryStatemachine) Get(key string) (string, error) {
	ms.RLock()
	defer ms.RUnlock()

	if v, ok := ms.data[key]; ok {
		return v, nil
	} else {
		return "", errNotExists
	}
}

func (ms *memoryStatemachine) Put(key, value string) error {
	kv := map[string]string{key: value}
	if data, err := json.Marshal(kv); err != nil {
		return err
	} else {
		resp := ms.raft.Submit(ms.id, data)
		_, err = resp.Response()
		if err != nil {
			return errors.New(fmt.Sprintf("Put error[%v].\r\n", err))
		}
		return nil
	}
}

func (ms *memoryStatemachine) AddNode(peer proto.Peer) error {
	resp := ms.raft.ChangeMember(ms.id, proto.ConfAddNode, peer, nil)
	_, err := resp.Response()
	if err != nil {
		return errors.New("AddNode error.")
	}
	return nil
}

func (ms *memoryStatemachine) RemoveNode(peer proto.Peer) error {
	resp := ms.raft.ChangeMember(ms.id, proto.ConfRemoveNode, peer, nil)
	_, err := resp.Response()
	if err != nil {
		return errors.New("RemoveNode error.")
	}
	return nil
}

func (ms *memoryStatemachine) setApplied(index uint64) {
	ms.Lock()
	defer ms.Unlock()
	ms.applied = index
}

type memorySnapshot struct {
	offset  int
	applied uint64
	data    []byte
}

func (s *memorySnapshot) Next() ([]byte, error) {
	if s.offset >= len(s.data) {
		return nil, io.EOF
	}
	s.offset = len(s.data)
	return s.data, nil
}

func (s *memorySnapshot) ApplyIndex() uint64 {
	return s.applied
}

func (s *memorySnapshot) Close() {
	return
}
