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

package raft

import (
	"master-server/raft/proto"
	"master-server/raft/util"
)

type MultiTransport struct {
	heartbeat *heartbeatTransport
	replicate *replicateTransport
}

func NewMultiTransport(raft *RaftServer, config *TransportConfig) (Transport, error) {
	mt := new(MultiTransport)

	if ht, err := newHeartbeatTransport(raft, config); err != nil {
		return nil, err
	} else {
		mt.heartbeat = ht
	}
	if rt, err := newReplicateTransport(raft, config); err != nil {
		return nil, err
	} else {
		mt.replicate = rt
	}

	mt.heartbeat.start()
	mt.replicate.start()
	return mt, nil
}

func (t *MultiTransport) Stop() {
	t.heartbeat.stop()
	t.replicate.stop()
}

func (t *MultiTransport) Send(m *proto.Message) {
	// if m.IsElectionMsg() {
	if m.IsHeartbeatMsg() {
		t.heartbeat.send(m)
	} else {
		t.replicate.send(m)
	}
}

func (t *MultiTransport) SendSnapshot(m *proto.Message, rs *snapshotStatus) {
	t.replicate.sendSnapshot(m, rs)
}

func reciveMessage(r *util.BufferReader) (msg *proto.Message, err error) {
	msg = proto.GetMessage()
	if err = msg.Decode(r); err != nil {
		proto.ReturnMessage(msg)
	}
	return
}
