package server

import (
	"time"

	"util/log"
	"model/pkg/metapb"
	"golang.org/x/net/context"
)

// 根据上报的
type FailoverWorker struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
}

func NewFailoverWorker(wm *WorkerManager, interval time.Duration) Worker {
	ctx, cancel := context.WithCancel(wm.ctx)
	return &FailoverWorker{
		name: failoverWorkerName,
		ctx: ctx,
		cancel: cancel,
		interval: interval}
}

func (f *FailoverWorker) GetName() string {
	return f.name
}

func (f *FailoverWorker) Work(cluster *Cluster) {
	for _, n := range cluster.GetAllNode() {
		select {
		case <-f.ctx.Done():
			return
		default:
		}

		//if n.LastHeartbeatTS.IsZero() {
		//	// 新load进来的节点，此时仍为旧状态或无状态，尚未调用GetNodeId，忽略掉
		//	continue
		//}

		switch n.State {
		case metapb.NodeState_N_Upgrade:
			continue
		case metapb.NodeState_N_Initial:
			continue
		case metapb.NodeState_N_Logout:
			continue
		case metapb.NodeState_N_Login:
			if time.Since(n.LastHeartbeatTS) > DefaultDownTimeLimit {
				cluster.UpdateNodeState(n, metapb.NodeState_N_Offline)
			}
		case metapb.NodeState_N_Offline:
			// TODO: 可以增加主动探活ds，以及添加metrics采集的ds错误统计条件
			if time.Since(n.LastHeartbeatTS) > 3 * DefaultDownTimeLimit {
				cluster.UpdateNodeState(n, metapb.NodeState_N_Tombstone)
				log.Warn("node[%s] is fault, we need failover", n.GetServerAddr())

				continue
			}
		case metapb.NodeState_N_Tombstone:
			// 所有的副本都迁移完成
			if n.GetRangesSize() == 0 && time.Since(n.LastHeartbeatTS) > cluster.opt.GetMaxNodeDownTime() {
				cluster.UpdateNodeState(n, metapb.NodeState_N_Logout)
				log.Warn("node[%s] is fault, we need failover", n.GetServerAddr())
				continue
			}
		}
	}

	return
}

func (f *FailoverWorker) AllowWork(cluster *Cluster) bool {
	if cluster.autoFailoverUnable {
		return false
	}
	return true
}

func (f *FailoverWorker) GetInterval() time.Duration {
	return f.interval
}

func (f *FailoverWorker) Stop() {
	f.cancel()
}

