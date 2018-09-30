// Copyright 2018 The TigLabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
