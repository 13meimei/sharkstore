package server

import (
	"time"
	"golang.org/x/net/context"
	"util/log"
	"model/pkg/metapb"
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
	rng, oldPeer, targetNodeId := selectRemovePeer(cluster, w.GetName())
	if rng == nil {
		log.Debug("no range need balance")
		return
	}

	id, err := cluster.GenId()
	if err != nil {
		return
	}
	newPeer, err := cluster.allocPeer(targetNodeId)
	if err != nil {
		log.Error("create peer nodeId:%d error:%s", targetNodeId, err.Error())
		return
	}
	cluster.metric.CollectScheduleCounter(w.GetName(), "new_operator")
	log.Debug("start to balance region and remove peer, region:[%v], old peer:[%v], old node:[%v], new node:[%v]",
		rng.GetId(), oldPeer.GetId(), oldPeer.GetNodeId(), targetNodeId)
	cluster.eventDispatcher.pushEvent(NewChangePeerEvent(id, rng, oldPeer, newPeer, w.GetName()))
	return
}

func (w *balanceNodeRangeWorker) AllowWork(cluster *Cluster) bool {
	if cluster.autoFailoverUnable {
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
func selectRemovePeer(cluster *Cluster, workerName string) (*Range, *metapb.Peer, uint64) {
	nodes := cluster.GetAllActiveNode()
	if len(nodes) == 0 {
		log.Debug("%v: node is nil", workerName)
		cluster.metric.CollectScheduleCounter(workerName, "no_node")
		return nil, nil, 0
	}

	newSelectors := []NodeSelector{
		NewWriterOpsThresholdSelector(cluster.opt),
		NewStorageThresholdSelector(cluster.opt),
		NewDifferCacheNodeSelector(cluster.hbManager.dealIngNodes),
	}

	mostRangeNode, leastRangeNode, force := SelectMostAndLeastRangeNode(cluster.opt, nodes, newSelectors)
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

	if !force && mostRangeNum-leastRangeNum < uint32(Min_range_balance_num) {
		log.Debug("mostNode %v mostRangeNum %v , leastNode %v leastRangeNum %v, don't need balance",
			mostRangeNode.GetId(), mostRangeNum, leastRangeNode.GetId(), leastRangeNum)
		return nil, nil, 0
	}

	//选节点的peer不在mostRangeNode的 range, 且该range  没有 peer在leastRangeNode 上
	var rng *Range
	for _, r := range mostRangeNode.GetAllRanges() {
		if r.GetLeader().GetNodeId() != mostRangeNode.GetId() && r.require(cluster) {
			ipSelector := NewDifferIPSelector(r.GetNodes(cluster))
			if !ipSelector.CanSelect(leastRangeNode) {
				continue
			}
			rng = r
			break
		}
	}

	if rng == nil {
		log.Debug("%v: select follower range than exclude leastRangeNode %v is nil ", workerName, leastRangeNode)
		for _, r := range mostRangeNode.GetAllRanges() {
			if r.GetLeader().GetNodeId() == mostRangeNode.GetId() && r.require(cluster) {
				ipSelector := NewDifferIPSelector(r.GetNodes(cluster))
				if !ipSelector.CanSelect(leastRangeNode) {
					continue
				}
				rng = r
				break
			}
		}
	}

	if rng == nil {
		log.Debug("%v: select leader range that exclude leastRangeNode %v is nil ", workerName, leastRangeNode)
		for _, r := range mostRangeNode.GetAllRanges() {
			if r.GetLeader().GetNodeId() != mostRangeNode.GetId() && r.require(cluster) {
				leastRangeNode = cluster.selectNodeForAddPeer(r)
				if leastRangeNode != nil {
					rng = r
					break
				}
			}
		}
	}

	if rng == nil {
		log.Debug("%v: select follow range to best node is nil  %v", workerName, leastRangeNode)
		for _, r := range mostRangeNode.GetAllRanges() {
			if r.GetLeader().GetNodeId() == mostRangeNode.GetId() && r.require(cluster)  {
				leastRangeNode = cluster.selectNodeForAddPeer(r)
				if leastRangeNode != nil {
					rng = r
					break
				}
			}
		}
	}

	if rng == nil {
		log.Debug("%v: select leader range to best node is nil  %v", workerName, leastRangeNode)
		cluster.metric.CollectScheduleCounter(workerName, "no_peer")
		cluster.hbManager.dealIngNodes.set(mostRangeNode.GetId())
		return nil, nil, 0
	}

	return rng, rng.GetNodePeer(mostRangeNode.GetId()), leastRangeNode.GetId()
}
