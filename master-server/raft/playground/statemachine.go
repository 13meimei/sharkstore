package main

import (
	"encoding/binary"
	"sync/atomic"

	"master-server/raft"
	"master-server/raft/proto"
	"util/log"
)

type stateMachine struct {
	nodeID uint64
	r      *resolver

	applied uint64

	sum uint64
}

func newStateMachine(nodeID uint64, r *resolver) *stateMachine {
	return &stateMachine{
		nodeID: nodeID,
		r:      r,
	}
}

func (sm *stateMachine) current() uint64 {
	return atomic.LoadUint64(&sm.sum)
}

func (sm *stateMachine) Apply(command []byte, index uint64) (interface{}, error) {
	u := binary.BigEndian.Uint64(command)
	atomic.AddUint64(&sm.sum, u)
	log.Info("[NODE: %d] sum increased %d, sum=%d", sm.nodeID, u, atomic.LoadUint64(&sm.sum))
	sm.applied = index
	return sm.sum, nil
}

func (sm *stateMachine) ApplyMemberChange(cc *proto.ConfChange, index uint64) (interface{}, error) {
	switch cc.Type {
	case proto.ConfAddNode:
		log.Info("[NODE: %d] add node: %d", sm.nodeID, cc.Peer.ID)
		sm.r.addNode(cc.Peer.ID)
	case proto.ConfRemoveNode:
		log.Info("[NODE: %d] remove node: %d", sm.nodeID, cc.Peer.ID)
		sm.r.removeNode(cc.Peer.ID)
	}
	return nil, nil
}

func (sm *stateMachine) Snapshot() (proto.Snapshot, error) {
	log.Info("snapshot creating. sum=%v, applied=%v", sm.sum, sm.applied)
	return newSnapshot(sm.sum, sm.applied), nil
}

func (sm *stateMachine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	buf, err := iter.Next()
	if err != nil {
		return err
	}
	sm.sum = binary.BigEndian.Uint64(buf)
	log.Info("recovered from snapshot. sum=%v", sm.sum)
	iter.Next()
	return nil
}

func (sm *stateMachine) HandleFatalEvent(err *raft.FatalError) {
	log.Panic("[NODE: %d] panic: %v", sm.nodeID, err.Err)
}

func (sm *stateMachine) HandleLeaderChange(leader uint64) {
	log.Info("[NODE: %d] leader change to: %d", sm.nodeID, leader)
}
