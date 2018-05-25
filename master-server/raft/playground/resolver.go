package main

import (
	"errors"
	"sync"

	"master-server/raft"
)

type resolver struct {
	nodes map[uint64]struct{}
	sync.Mutex
}

func newResolver() *resolver {
	return &resolver{nodes: make(map[uint64]struct{})}
}

func (r *resolver) addNode(nodeID uint64) {
	r.Lock()
	r.nodes[nodeID] = struct{}{}
	r.Unlock()
}

func (r *resolver) removeNode(nodeID uint64) {
	r.Lock()
	delete(r.nodes, nodeID)
	r.Unlock()
}

func (r *resolver) AllNodes() (all []uint64) {
	r.Lock()
	for k := range r.nodes {
		all = append(all, k)
	}
	r.Unlock()
	return
}

func (r *resolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	raddr, ok := addrDatabase[nodeID]
	if !ok {
		return "", errors.New("no such node")
	}
	switch stype {
	case raft.HeartBeat:
		return "127.0.0.1" + raddr.heartbeat, nil
	case raft.Replicate:
		return "127.0.0.1" + raddr.replicate, nil
	default:
		return "", errors.New("unknown socket type")
	}
}
