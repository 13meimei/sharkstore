package raft

import (
	"math"

	"master-server/raft/logger"
	"master-server/raft/proto"
)

func (r *raftFsm) becomeFollower(term, lead uint64) {
	r.step = stepFollower
	r.reset(term, 0, false)
	r.tick = r.tickElection
	r.leader = lead
	r.state = stateFollower
	if logger.IsEnableInfo() {
		logger.Info("raft[%v] became follower at term[%d].", r.id, r.term)
	}
}

func stepFollower(r *raftFsm, m *proto.Message) {
	switch m.Type {
	case proto.LocalMsgProp:
		if r.leader == NoLeader {
			if logger.IsEnableWarn() {
				logger.Warn("raft[%v] no leader at term %d; dropping proposal.", r.id, r.term)
			}
			return
		}
		m.To = r.leader
		r.send(m)
		return

	case proto.ReqMsgAppend:
		r.electionElapsed = 0
		r.leader = m.From
		r.handleAppendEntries(m)
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgHeartBeat:
		r.electionElapsed = 0
		r.leader = m.From
		return

	case proto.ReqMsgElectAck:
		r.electionElapsed = 0
		r.leader = m.From
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgElectAck
		nmsg.To = m.From
		r.send(nmsg)
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgVote:
		fpri, lpri := uint16(math.MaxUint16), uint16(0)
		if pr, ok := r.replicas[m.From]; ok {
			fpri = pr.peer.Priority
		}
		if pr, ok := r.replicas[r.config.NodeID]; ok {
			lpri = pr.peer.Priority
		}

		if (!r.config.LeaseCheck || r.leader == NoLeader) && (r.vote == NoLeader || r.vote == m.From) && r.raftLog.isUpToDate(m.Index, m.LogTerm, fpri, lpri) {
			r.electionElapsed = 0
			if logger.IsEnableInfo() {
				logger.Info("raft[%v] [logterm: %d, index: %d, vote: %v] voted for %v [logterm: %d, index: %d] at term %d.", r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.From, m.LogTerm, m.Index, r.term)
			}
			r.vote = m.From
			nmsg := proto.GetMessage()
			nmsg.Type = proto.RespMsgVote
			nmsg.To = m.From
			r.send(nmsg)
		} else {
			if logger.IsEnableInfo() {
				logger.Info("raf[%v] [logterm: %d, index: %d, vote: %v] rejected vote from %v [logterm: %d, index: %d] at term %d.", r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.From, m.LogTerm, m.Index, r.term)
			}
			nmsg := proto.GetMessage()
			nmsg.Type = proto.RespMsgVote
			nmsg.To = m.From
			nmsg.Reject = true
			r.send(nmsg)
		}
		proto.ReturnMessage(m)
		return

	case proto.LeaseMsgTimeout:
		if r.leader == m.From {
			r.electionElapsed = 0
			nmsg := proto.GetMessage()
			nmsg.Type = proto.LocalMsgHup
			nmsg.From = r.config.NodeID
			r.Step(nmsg)
		}
		proto.ReturnMessage(m)
		return
	}
}

func (r *raftFsm) tickElection() {
	if !r.promotable() {
		r.electionElapsed = 0
		return
	}

	r.electionElapsed++
	timeout := false
	// check follower lease (2 * electiontimeout)
	if r.config.LeaseCheck && r.leader != NoLeader && r.state == stateFollower {
		timeout = (r.electionElapsed >= (r.config.ElectionTick << 1))
	} else {
		timeout = r.pastElectionTimeout()
	}
	if timeout {
		r.electionElapsed = 0
		m := proto.GetMessage()
		m.Type = proto.LocalMsgHup
		m.From = r.config.NodeID
		r.Step(m)
	}
}

func (r *raftFsm) handleAppendEntries(m *proto.Message) {
	if m.Index < r.raftLog.committed {
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgAppend
		nmsg.To = m.From
		nmsg.Index = r.raftLog.committed
		nmsg.Commit = r.raftLog.committed
		r.send(nmsg)
		return
	}

	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgAppend
		nmsg.To = m.From
		nmsg.Index = mlastIndex
		nmsg.Commit = r.raftLog.committed
		r.send(nmsg)
	} else {
		if logger.IsEnableDebug() {
			logger.Debug("raft[%v] [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %v",
				r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		}
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgAppend
		nmsg.To = m.From
		nmsg.Index = m.Index
		nmsg.Commit = r.raftLog.committed
		nmsg.Reject = true
		nmsg.RejectIndex = r.raftLog.lastIndex()
		r.send(nmsg)
	}
}

func (r *raftFsm) promotable() bool {
	_, ok := r.replicas[r.config.NodeID]
	return ok
}
