package server

import (
	"util/log"
	"time"
	"model/pkg/metapb"
	"github.com/gogo/protobuf/proto"
	"fmt"
)

var (
	DefaultMaxNodeDownTimeInterval time.Duration = 60 * time.Second
	DefaultMaxPeerDownTimeInterval time.Duration = 2 * 60 * time.Second

	// 大于一个调度周期+一个心跳周期，预留冗余
	DefaultChangeLeaderTimeout time.Duration = time.Second * time.Duration(30)
	DefaultDelRangeTimeout     time.Duration = time.Second * time.Duration(30)
	DefaultAddPeerTimeout      time.Duration = time.Second * time.Duration(300)
	DefaultDelPeerTimeout      time.Duration = time.Second * time.Duration(30)
)

type hb_range_manager struct {
	cluster      *Cluster
	dealIngNodes *idCache
}

func NewHBRangeManager(cluster *Cluster) *hb_range_manager {
	dealIngNodes := newIDCache(time.Second, 10 * time.Second)
	return &hb_range_manager{cluster: cluster, dealIngNodes: dealIngNodes,}
}

type RunMode int

const (
	RUN_MODE_INIT   RunMode = iota //永远不要被用到
	RUN_MODE_LOCAL                 //在master执行
	RUN_MODE_REMOTE                //应答给DS执行
)

func (manager *hb_range_manager) isMorePeerDown(r *Range) bool {
	return len(r.DownPeers)*2 > len(r.GetPeers())
}

func peerGC(cluster *Cluster, r *metapb.Range, peer *metapb.Peer) error {
	node := cluster.FindNodeById(peer.GetNodeId())
	replica := &metapb.Replica{RangeId: r.GetId(), Peer: peer, StartKey: r.GetStartKey(), EndKey: r.GetEndKey()}
	v, _ := proto.Marshal(replica)
	key := []byte(fmt.Sprintf("%s%d", PREFIX_REPLICA, peer.GetId()))
	err := cluster.store.Put(key, v)
	if err != nil {
		return err
	}
	node.AddTrashReplica(replica)
	return nil
}

func (manager *hb_range_manager) CheckRange(cluster *Cluster, rng *Range) RangeEvent {

	// 启动故障恢复或者分片需要删除,优先处理故障的副本
	if cluster.autoFailoverUnable {
		log.Debug("can not failover")
		return nil
	}

	// 处理分片回收
	if rng.State == metapb.RangeState_R_Remove {
		//TODO: 直接调用DS接口删除range 然后回收
		id, err := cluster.GenId()
		if err != nil {
			return nil
		}
		return NewDelRangeEvent(id, rng.GetId(), "hb range remove")
	}

	//down peer 处理
	if event := manager.checkDownPeer(cluster, rng); event != nil {
		return event
	}

	if len(rng.GetPeers()) < cluster.opt.GetMaxReplicas() {
		log.Info("range %d peer %d less than %d", rng.GetId(), len(rng.GetPeers()), cluster.opt.GetMaxReplicas())
		newPeer, err := cluster.allocPeerAndSelectNode(rng)

		if err != nil {
			log.Error("rangeId:%d,%s", rng.GetId(), err.Error())
			return nil
		}

		id, err := cluster.GenId()
		if err != nil {
			log.Error("rangeId:%d,%s", rng.GetId(), err.Error())
			return nil
		}
		return NewAddPeerEvent(id, rng.GetId(), newPeer, "hb less peer")
	}

	if len(rng.GetPeers()) > cluster.opt.GetMaxReplicas() {
		log.Info("range %d peer %d more than %d", rng.GetId(), len(rng.GetPeers()), cluster.opt.GetMaxReplicas())

		if len(rng.GetPendingPeers()) != 0 {
			log.Info("range %v peer number %v / pending peer number %v: ", rng.GetId(), len(rng.GetPeers()), len(rng.GetPendingPeers()))
			return nil
		}
		// 优先下掉ip相同的副本
		oldPeer := cluster.selectWorstPeer(rng)
		if oldPeer == nil {
			return nil
		}
		id, err := cluster.GenId()
		if err != nil {
			return nil
		}
		return manager.createDelPeerEvent(id, rng, oldPeer, "hb more peer")
	}

	// 检查是否有ip相同的副本
	if ip, ok := cluster.checkSameIpNode(rng.GetNodes(cluster)); ok {
		//先添加，后面再自动删除
		log.Info("range %d exist same ip %v", rng.GetId(), ip)
		newPeer, err := cluster.allocPeerAndSelectNode(rng)

		if err != nil {
			log.Error("rangeId:%d,%s", rng.GetId(), err.Error())
			return nil
		}

		id, err := cluster.GenId()
		if err != nil {
			log.Error("rangeId:%d,%s", rng.GetId(), err.Error())
			return nil
		}
		return NewAddPeerEvent(id, rng.GetId(), newPeer, "hb same IP")
	}

	return nil
}

func (manager *hb_range_manager) checkDownPeer(cluster *Cluster, rng *Range) RangeEvent {
	if manager.isMorePeerDown(rng) {
		log.Warn("range %v more down peers ", rng)
	}
	for _, stats := range rng.DownPeers {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}

		if stats.GetDownSeconds() < uint64(DefaultMaxPeerDownTimeInterval.Seconds()) {
			continue
		}
		id, err := cluster.GenId()
		if err != nil {
			return nil
		}
		return manager.createDelPeerEvent(id, rng, peer, "hb down peer")
	}
	return nil
}

func (manager *hb_range_manager) createDelPeerEvent(id uint64, rng *Range, peer *metapb.Peer, creator string) RangeEvent {
	delPeerEvent := NewDelPeerEvent(id, rng.GetId(), peer, creator)

	if rng.GetLeader() != nil && rng.GetLeader().GetId() == peer.GetId() {
		//TODO:应该选一个最好的follower进行切换
		if follower := rng.GetRandomFollower(); follower != nil {
			changeLeaderEvent := NewTryChangeLeaderEvent(id, rng.GetId(), rng.GetLeader(), follower, creator)
			changeLeaderEvent.next = delPeerEvent
			return changeLeaderEvent
		}
		return nil
	}
	return delPeerEvent
}
