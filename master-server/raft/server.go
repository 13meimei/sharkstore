package raft

import (
	"context"
	"errors"
	"sync"
	"time"

	"master-server/raft/logger"
	"master-server/raft/proto"
	"master-server/raft/util"
)

const heartbeatWarnDelay = time.Millisecond * 5

var (
	fatalStopc = make(chan uint64)
)

type RaftServer struct {
	config *Config
	ticker *time.Ticker
	heartc chan *proto.Message
	stopc  chan struct{}
	mu     sync.RWMutex
	rafts  map[uint64]*raft

	heartStat struct {
		last         time.Time
		lastTake     time.Duration
		maxSize      uint64
		maxSizeTo    uint64
		maxSizeRafts int
	}
}

func NewRaftServer(config *Config) (*RaftServer, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	rs := &RaftServer{
		config: config,
		ticker: time.NewTicker(config.TickInterval),
		rafts:  make(map[uint64]*raft),
		heartc: make(chan *proto.Message, 512),
		stopc:  make(chan struct{}),
	}
	rs.heartStat.last = time.Now()

	if transport, err := NewMultiTransport(rs, &config.TransportConfig); err != nil {
		return nil, err
	} else {
		rs.config.transport = transport
	}

	util.RunWorkerUtilStop(rs.run, rs.stopc)
	return rs, nil
}

func (rs *RaftServer) run() {
	ticks := 0
	for {
		select {
		case <-rs.stopc:
			return

		case id := <-fatalStopc:
			rs.mu.Lock()
			delete(rs.rafts, id)
			rs.mu.Unlock()

		case m := <-rs.heartc:
			switch m.Type {
			case proto.ReqMsgHeartBeat:
				rs.handleHeartbeat(m)
			case proto.RespMsgHeartBeat:
				rs.handleHeartbeatResp(m)
			}

		case <-rs.ticker.C:
			ticks++
			if ticks >= rs.config.HeartbeatTick {
				ticks = 0
				start := time.Now()
				rs.sendHeartbeat()
				rs.heartStat.lastTake = time.Since(start)
				rs.checkHeartDelay()
			}

			rs.mu.RLock()
			for _, raft := range rs.rafts {
				raft.tick()
			}
			rs.mu.RUnlock()
		}
	}
}

func (rs *RaftServer) Stop() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	select {
	case <-rs.stopc:
		return

	default:
		close(rs.stopc)
		rs.ticker.Stop()
		wg := new(sync.WaitGroup)
		for id, s := range rs.rafts {
			delete(rs.rafts, id)
			wg.Add(1)
			go func(r *raft) {
				defer wg.Done()
				r.stop()
			}(s)
		}
		wg.Wait()
		rs.config.transport.Stop()
	}
}

func (rs *RaftServer) CreateRaft(raftConfig *RaftConfig) error {
	var (
		raft *raft
		err  error
	)

	defer func() {
		if err != nil {
			logger.Error("CreateRaft [%v] failed, error is:\r\n %s", raftConfig.ID, err.Error())
		}
	}()

	if raft, err = newRaft(rs.config, raftConfig); err != nil {
		return err
	}
	if raft == nil {
		err = errors.New("CreateRaft return nil, maybe occur panic.")
		return err
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()
	if _, ok := rs.rafts[raftConfig.ID]; ok {
		raft.stop()
		err = ErrRaftExists
		return err
	}
	rs.rafts[raftConfig.ID] = raft
	return nil
}

func (rs *RaftServer) RemoveRaft(id uint64) error {
	rs.mu.Lock()
	raft, ok := rs.rafts[id]
	delete(rs.rafts, id)
	rs.mu.Unlock()

	if ok {
		raft.stop()
	}

	logger.Info("raft[%d] removed", id)

	return nil
}

func (rs *RaftServer) Submit(ctx context.Context, id uint64, cmd []byte) (future *Future) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	future = newFuture(ctx)
	if !ok {
		future.respond(nil, ErrRaftNotExists)
		return
	}
	raft.propose(cmd, future)
	return
}

func (rs *RaftServer) ChangeMember(ctx context.Context, id uint64, changeType proto.ConfChangeType, peer proto.Peer, context []byte) (future *Future) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	future = newFuture(ctx)
	if !ok {
		future.respond(nil, ErrRaftNotExists)
		return
	}
	raft.proposeMemberChange(&proto.ConfChange{Type: changeType, Peer: peer, Context: context}, future)
	return
}

func (rs *RaftServer) Status(id uint64) (status *Status) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		status = raft.status()
	}
	if status == nil {
		status = &Status{
			ID:      id,
			NodeID:  rs.config.NodeID,
			Stopped: true,
		}
	}
	return
}

func (rs *RaftServer) LeaderTerm(id uint64) (leader, term uint64) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		return raft.leaderTerm()
	}
	return NoLeader, 0
}

func (rs *RaftServer) IsLeader(id uint64) bool {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		return raft.isLeader()
	}
	return false
}

func (rs *RaftServer) AppliedIndex(id uint64) uint64 {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		return raft.applied()
	}
	return 0
}

func (rs *RaftServer) TryToLeader(ctx context.Context, id uint64) (future *Future) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	future = newFuture(ctx)
	if !ok {
		future.respond(nil, ErrRaftNotExists)
		return
	}
	raft.tryToLeader(future)
	return
}

func (rs *RaftServer) Truncate(id uint64, index uint64) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if !ok {
		return
	}
	raft.truncate(index)
}

func (rs *RaftServer) GetUnreachable(id uint64) (nodes []uint64) {
	downReplicas := rs.GetDownReplicas(id)
	for _, r := range downReplicas {
		nodes = append(nodes, r.NodeID)
	}
	return
}

// GetDownReplicas 获取down的副本
func (rs *RaftServer) GetDownReplicas(id uint64) (downReplicas []DownReplica) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if !ok {
		return nil
	}

	status := raft.status()
	if status != nil && len(status.Replicas) > 0 {
		for n, r := range status.Replicas {
			if n == rs.config.NodeID {
				continue
			}
			since := time.Since(r.LastActive)
			// 两次心跳内没活跃就视为Down
			downDuration := since - time.Duration(2*rs.config.HeartbeatTick)*rs.config.TickInterval
			if downDuration > 0 {
				downReplicas = append(downReplicas, DownReplica{
					NodeID:      n,
					DownSeconds: int(downDuration / time.Second),
				})
			}
		}
	}
	return
}

// GetPendingReplica get snapshot pending followers
func (rs *RaftServer) GetPendingReplica(id uint64) (peers []uint64) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if !ok {
		return nil
	}

	status := raft.status()
	if status != nil && len(status.Replicas) > 0 {
		for n, r := range status.Replicas {
			if n == rs.config.NodeID {
				continue
			}
			if r.Snapshoting {
				peers = append(peers, n)
			}
		}
	}
	return
}

func (rs *RaftServer) checkHeartDelay() {
	now := time.Now()
	delay := now.Sub(rs.heartStat.last) - rs.config.TickInterval*time.Duration(rs.config.HeartbeatTick)
	if delay > heartbeatWarnDelay {
		logger.Warn("raft heartbeat delayed %v, maxsize: %d, to: %d, rafts: %d, lastTake: %v", delay,
			rs.heartStat.maxSize, rs.heartStat.maxSizeTo, rs.heartStat.maxSizeRafts, rs.heartStat.lastTake)

		// 重置（已经输出过了）
		rs.heartStat.maxSize = 0
		rs.heartStat.maxSizeTo = 0
		rs.heartStat.maxSizeRafts = 0
	}
	rs.heartStat.last = now
}

func (rs *RaftServer) sendHeartbeat() {
	// key: sendto nodeId; value: range ids
	nodes := make(map[uint64]proto.HeartbeatContext)
	rs.mu.RLock()
	for id, raft := range rs.rafts {
		if !raft.isLeader() {
			continue
		}
		peers := raft.getPeers()
		for _, p := range peers {
			nodes[p] = append(nodes[p], id)
		}
	}
	rs.mu.RUnlock()

	for to, ctx := range nodes {
		if to == rs.config.NodeID {
			continue
		}

		msg := proto.GetMessage()
		msg.Type = proto.ReqMsgHeartBeat
		msg.From = rs.config.NodeID
		msg.To = to
		msg.Context = proto.EncodeHBConext(ctx)
		if msg.Size() > rs.heartStat.maxSize {
			rs.heartStat.maxSize = msg.Size()
			rs.heartStat.maxSizeTo = to
			rs.heartStat.maxSizeRafts = len(ctx)
		}
		rs.config.transport.Send(msg)
	}
}

func (rs *RaftServer) handleHeartbeat(m *proto.Message) {
	ctx := proto.DecodeHBContext(m.Context)
	var respCtx proto.HeartbeatContext
	rs.mu.RLock()
	for _, id := range ctx {
		if raft, ok := rs.rafts[id]; ok {
			raft.reciveMessage(m)
			respCtx = append(respCtx, id)
		}
	}
	rs.mu.RUnlock()

	msg := proto.GetMessage()
	msg.Type = proto.RespMsgHeartBeat
	msg.From = rs.config.NodeID
	msg.To = m.From
	msg.Context = proto.EncodeHBConext(respCtx)
	rs.config.transport.Send(msg)
}

func (rs *RaftServer) handleHeartbeatResp(m *proto.Message) {
	ctx := proto.DecodeHBContext(m.Context)

	rs.mu.RLock()
	defer rs.mu.RUnlock()

	for _, id := range ctx {
		if raft, ok := rs.rafts[id]; ok {
			raft.reciveMessage(m)
		}
	}
}

func (rs *RaftServer) reciveMessage(m *proto.Message) {
	if m.Type == proto.ReqMsgHeartBeat || m.Type == proto.RespMsgHeartBeat {
		rs.heartc <- m
		return
	}

	rs.mu.RLock()
	raft, ok := rs.rafts[m.ID]
	rs.mu.RUnlock()
	if ok {
		raft.reciveMessage(m)
	}
}

func (rs *RaftServer) reciveSnapshot(req *snapshotRequest) {
	rs.mu.RLock()
	raft, ok := rs.rafts[req.header.ID]
	rs.mu.RUnlock()

	if !ok {
		req.respond(ErrRaftNotExists)
		return
	}
	raft.reciveSnapshot(req)
}
