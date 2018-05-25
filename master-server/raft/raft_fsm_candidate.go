package raft

import (
	"fmt"

	"master-server/raft/logger"
	"master-server/raft/proto"
)

func (r *raftFsm) becomeCandidate() {
	if r.state == stateLeader {
		panic(AppPanicError(fmt.Sprintf("[raft->becomeCandidate][%v] invalid transition [leader -> candidate].", r.id)))
	}

	r.step = stepCandidate
	r.reset(r.term+1, 0, false)
	r.tick = r.tickElection
	r.vote = r.config.NodeID
	r.state = stateCandidate

	if logger.IsEnableInfo() {
		logger.Info("raft[%v] became candidate at term %d.", r.id, r.term)
	}
}

func stepCandidate(r *raftFsm, m *proto.Message) {
	switch m.Type {
	case proto.LocalMsgProp:
		if logger.IsEnableDebug() {
			logger.Debug("raft[%v] no leader at term %d; dropping proposal.", r.id, r.term)
		}
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgAppend:
		r.becomeFollower(r.term, m.From)
		r.handleAppendEntries(m)
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgHeartBeat:
		r.becomeFollower(r.term, m.From)
		return

	case proto.ReqMsgElectAck:
		r.becomeFollower(r.term, m.From)
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgElectAck
		nmsg.To = m.From
		r.send(nmsg)
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgVote:
		if logger.IsEnableInfo() {
			logger.Info("raft[%v] [logterm: %d, index: %d, vote: %v] rejected vote from %v [logterm: %d, index: %d] at term %d.", r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.From, m.LogTerm, m.Index, r.term)
		}
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgVote
		nmsg.To = m.From
		nmsg.Reject = true
		r.send(nmsg)
		proto.ReturnMessage(m)
		return

	case proto.RespMsgVote:
		gr := r.poll(m.From, !m.Reject)
		if logger.IsEnableInfo() {
			logger.Info("raft[%v] [q:%d] has received %d votes and %d vote rejections.", r.id, r.quorum(), gr, len(r.votes)-gr)
		}
		switch r.quorum() {
		case gr:
			if r.config.LeaseCheck {
				r.becomeElectionAck()
			} else {
				r.becomeLeader()
				r.bcastAppend()
			}
		case len(r.votes) - gr:
			r.becomeFollower(r.term, NoLeader)
		}
	}
}

func (r *raftFsm) campaign(force bool) {
	r.becomeCandidate()
	if r.quorum() == r.poll(r.config.NodeID, true) {
		if r.config.LeaseCheck {
			r.becomeElectionAck()
		} else {
			r.becomeLeader()
		}
		return
	}

	for id := range r.replicas {
		if id == r.config.NodeID {
			continue
		}
		li, lt := r.raftLog.lastIndexAndTerm()
		if logger.IsEnableDebug() {
			logger.Debug("raft[%v] campaign: [logterm: %d, index: %d] sent vote request to %v at term %d.", r.id, lt, li, id, r.term)
		}

		m := proto.GetMessage()
		m.To = id
		m.Type = proto.ReqMsgVote
		m.ForceVote = force
		m.Index = li
		m.LogTerm = lt
		r.send(m)
	}
}

func (r *raftFsm) poll(id uint64, v bool) (granted int) {
	if logger.IsEnableDebug() {
		if v {
			logger.Debug("raft[%v] received vote from %v at term %d.", r.id, id, r.term)
		} else {
			logger.Debug("raft[%v] received vote rejection from %v at term %d.", r.id, id, r.term)
		}
	}
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}
