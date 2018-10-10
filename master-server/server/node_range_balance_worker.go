package server

import (
	"golang.org/x/net/context"
	"model/pkg/metapb"
	"time"
	"util/log"
)

var (
	Min_range_balance_num = 10
)

type balanceNodeRangeWorker struct {
	opt      *scheduleOption
	limit    uint64
	name     string
	ctx      context.Context
	cancel   context.CancelFunc
	interval time.Duration
}

func NewBalanceNodeRangeWorker(wm *WorkerManager, interval time.Duration) *balanceNodeRangeWorker {
	ctx, cancel := context.WithCancel(wm.ctx)
	return &balanceNodeRangeWorker{
		name:     balanceRangeWorkerName,
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
	}
}
func (w *balanceNodeRangeWorker) GetName() string {
	return w.name
}

func (w *balanceNodeRangeWorker) Work(cluster *Cluster) {
	log.Debug("start %s", w.GetName())
	cluster.metric.CollectScheduleCounter(w.GetName(), "schedule")
	rng, oldPeer, targetNodeId := w.selectRemovePeer(cluster)
	if rng == nil || oldPeer == nil {
		log.Debug("no range need balance")
		return
	}

	newPeer, err := cluster.allocPeerAndSelectNode(rng, true, true)
	if newPeer == nil || err != nil {
		cluster.metric.CollectScheduleCounter(w.GetName(), "no_peer")
		log.Error("alloc peer failure rngId:%d err:%s", rng.GetId(), err.Error())
		return
	}

	id, err := cluster.GenId()
	if err != nil {
		return
	}
	cluster.metric.CollectScheduleCounter(w.GetName(), "new_operator")
	log.Debug("start to balance region and remove peer, region:[%v], old peer:[%v], old node:[%v], excepted new node:[%v]",
		rng.GetId(), oldPeer.GetId(), oldPeer.GetNodeId(), targetNodeId)
	tc := NewTransferPeerTasks(id, rng, "balance-range-transfer", oldPeer, newPeer)
	// TODO: check return
	cluster.taskManager.Add(tc)
	return
}

func (w *balanceNodeRangeWorker) AllowWork(cluster *Cluster) bool {
	if cluster.autoTransferUnable {
		return false
	}
	return true
}

func (w *balanceNodeRangeWorker) GetInterval() time.Duration {
	return w.interval
}

func (w *balanceNodeRangeWorker) Stop() {
	w.cancel()
}

// selectRemovePeer schedule to transfer the range by removing the peer
// for balancing the node range
func (w *balanceNodeRangeWorker) selectRemovePeer(cluster *Cluster) (*Range, *metapb.Peer, uint64) {
	nodes := cluster.GetAllActiveNode()
	if len(nodes) == 0 {
		log.Debug("%v: active node is nil", w.GetName())
		cluster.metric.CollectScheduleCounter(w.GetName(), "no_node")
		return nil, nil, 0
	}

	newSelectors := []NodeSelector{
		NewWriterOpsThresholdSelector(cluster.opt),
		NewStorageThresholdSelector(cluster.opt),
		NewSnapshotReceiveThresholdSelector(cluster.opt),
		NewDifferCacheNodeSelector(cluster.hbManager.dealIngNodes),
	}

	mostRangeNode, leastRangeNode, force, avgRangerNum := SelectMostAndLeastRangeNode(cluster.opt, nodes, newSelectors)
	var mostRangeNum, leastRangeNum = uint32(0), uint32(0)
	if mostRangeNode != nil {
		mostRangeNum = mostRangeNode.GetRangesCount()
	}
	if leastRangeNode != nil {
		leastRangeNum = leastRangeNode.GetRangesCount()
	} else {
		log.Debug("no target node ")
		return nil, nil, 0
	}

	balanceThreshold := maxFloat64(avgRangerNum/20, float64(Min_range_balance_num))
	if !force && float64(mostRangeNum-leastRangeNum) < balanceThreshold {
		log.Debug("mostNode %v mostRangeNum %v , leastNode %v leastRangeNum %v, don't need balance",
			mostRangeNode.GetId(), mostRangeNum, leastRangeNode.GetId(), leastRangeNum)
		return nil, nil, 0
	}

	//以下为初步检查是否有可用的目标node
	//优先选mostRangeNode上非leader的分片，迁移到leastRangeNode 上
	var rng *Range
	for _, r := range mostRangeNode.GetAllRanges() {
		if r.GetLeader().GetNodeId() != mostRangeNode.GetId() && r.require(cluster) {
			ipSelector := NewDifferIPSelector(r.GetNodes(cluster))
			if !ipSelector.CanSelect(leastRangeNode) {
				continue
			}
			leaderNode := cluster.FindNodeById(r.GetLeader().GetNodeId())
			if uint64(leaderNode.GetSendingSnapCount()) > cluster.opt.GetMaxSnapshotCount() {
				continue
			}
			rng = r
			break
		}
	}

	//选mostRangeNode上leader的分片，迁移到leastRangeNode 上
	if rng == nil {
		log.Debug("%v: select follower range than exclude leastRangeNode %v is nil ", w.GetName(), leastRangeNode)
		for _, r := range mostRangeNode.GetAllRanges() {
			if r.GetLeader().GetNodeId() == mostRangeNode.GetId() && r.require(cluster) {
				ipSelector := NewDifferIPSelector(r.GetNodes(cluster))
				if !ipSelector.CanSelect(leastRangeNode) {
					continue
				}
				if uint64(mostRangeNode.GetSendingSnapCount()) > cluster.opt.GetMaxSnapshotCount(){
					continue
				}
				rng = r
				break
			}
		}
	}

	if rng == nil {
		log.Debug("%v: select leader range that exclude leastRangeNode %v is nil ", w.GetName(), leastRangeNode)
		for _, r := range mostRangeNode.GetAllRanges() {
			if r.GetLeader().GetNodeId() != mostRangeNode.GetId() && r.require(cluster) {
				leastRangeNode = cluster.selectNodeForAddPeer(r, true)
				if leastRangeNode != nil {
					rng = r
					break
				}
			}
		}
	}

	if rng == nil {
		log.Debug("%v: select follow range to best node is nil  %v", w.GetName(), leastRangeNode)
		for _, r := range mostRangeNode.GetAllRanges() {
			if r.GetLeader().GetNodeId() == mostRangeNode.GetId() && r.require(cluster) {
				leastRangeNode = cluster.selectNodeForAddPeer(r, true)
				if leastRangeNode != nil {
					rng = r
					break
				}
			}
		}
	}

	if rng == nil {
		log.Debug("%v: select leader range to best node is nil  %v", w.GetName(), leastRangeNode)
		cluster.metric.CollectScheduleCounter(w.GetName(), "no_peer")
		cluster.hbManager.dealIngNodes.set(mostRangeNode.GetId())
		return nil, nil, 0
	}

	return rng, rng.GetNodePeer(mostRangeNode.GetId()), leastRangeNode.GetId()
}