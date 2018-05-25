package raft

import (
	"fmt"
	"time"
)

// DownReplica  down replica
type DownReplica struct {
	NodeID      uint64
	DownSeconds int
}

// ReplicaStatus  replica status
type ReplicaStatus struct {
	Match       uint64 // 复制进度
	Commit      uint64 // commmit位置
	Next        uint64
	State       string
	Snapshoting bool
	Paused      bool
	Active      bool
	LastActive  time.Time
	Inflight    int
}

// Status raft status
type Status struct {
	ID                uint64
	NodeID            uint64
	Leader            uint64
	Term              uint64
	Index             uint64
	Commit            uint64
	Applied           uint64
	Vote              uint64
	PendQueue         int
	RecvQueue         int
	AppQueue          int
	Stopped           bool
	RestoringSnapshot bool
	State             string // leader、follower、candidate
	Replicas          map[uint64]*ReplicaStatus
}

func (s *Status) String() string {
	st := "running"
	if s.Stopped {
		st = "stopped"
	} else if s.RestoringSnapshot {
		st = "snapshot"
	}
	j := fmt.Sprintf(`{"id":"%v","nodeID":"%v","state":"%v","leader":"%v","term":"%v","index":"%v","commit":"%v","applied":"%v","vote":"%v","pendingQueue":"%v",
					"recvQueue":"%v","applyQueue":"%v","status":"%v","replication":{`, s.ID, s.NodeID, s.State, s.Leader, s.Term, s.Index, s.Commit, s.Applied, s.Vote, s.PendQueue, s.RecvQueue, s.AppQueue, st)
	if len(s.Replicas) == 0 {
		j += "}}"
	} else {
		for k, v := range s.Replicas {
			p := "false"
			if v.Paused {
				p = "true"
			}
			subj := fmt.Sprintf(`"%v":{"match":"%v","commit":"%v","next":"%v","state":"%v","paused":"%v","inflight":"%v","active":"%v"},`, k, v.Match, v.Commit, v.Next, v.State, p, v.Inflight, v.Active)
			j += subj
		}
		j = j[:len(j)-1] + "}}"
	}
	return j
}
