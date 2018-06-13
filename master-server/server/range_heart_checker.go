package server

import (
	client "pkg-go/ds_client"
	"time"

	"golang.org/x/net/context"

	"model/pkg/metapb"
	"model/pkg/mspb"
	"model/pkg/taskpb"
	"util"
	"util/deepcopy"
	"util/log"
)

// RangeHeartChecker  check range heartbeat
type RangeHeartChecker struct {
	cluster *Cluster
}

// NewRangeHeartChecker new range heart checker
func NewRangeHeartChecker(cluster *Cluster) *RangeHeartChecker {
	return &RangeHeartChecker{
		cluster: cluster,
	}
}

func (c *RangeHeartChecker) isQuorumDown(r *Range) bool {
	totalVoters := 0
	downVoters := 0
	for _, peer := range r.GetPeers() {
		if peer.GetType() != metapb.PeerType_PeerType_Learner {
			totalVoters++
		}
	}
	for _, down := range r.GetDownPeers() {
		if down.Peer.GetType() != metapb.PeerType_PeerType_Learner {
			downVoters++
		}
	}
	quorum := totalVoters/2 + 1
	return totalVoters-downVoters < quorum
}

func (c *RangeHeartChecker) needToStore(r *Range) bool {
	return true
}

func (c *RangeHeartChecker) validate(req *mspb.RangeHeartbeatRequest, from string) bool {
	// meta must be not nil
	meta := req.GetRange()
	if mta == nil {
		log.Warn("[HB] invalid range: <nil> from %s", from)
		return false
	}

	// range is deleted
	if _, found := cluster.deletedRanges.FindRange(r.GetId()); found {
		log.Error("range[%v] had been deleted, but still exist heartbeat from nodeId[%d]. Please check ds log for more detail!",
			meta.GetId(), req.GetLeader().GetNodeId())
		return false
	}

	return true
}

// Check check range heartbeat
func (c *RangeHeartChecker) Check(ctx context.Context, req *mspb.RangeHeartbeatRequest) *taskpb.Task {
	from := util.GetIpFromContext(ctx)
	if !c.validate(req, from) {
		return nil
	}

	rng := c.cluster.FindRange(req.GetRange().GetId())

	// update range states
	persist, err := rng.Update(req)
	if err != nil {
		log.Error("range[%v] update from heartbeat error: %v", err)
		return nil
	}
	// persist range meta to store
	if persist {
		err := c.cluster.storeRange(rng.GetMeta())
		if err != nil {
			log.Error("store range[%s] failed, err[%v]", rng.SString(), err)
			return nil
		}
	}

	return nil
}

func prepareAddPeer(cluster *Cluster, r *Range, peer *metapb.Peer) error {
	rng := deepcopy.Iface(r.Range).(*metapb.Range)
	nodeId := peer.GetNodeId()
	node := cluster.FindNodeById(nodeId)
	if node == nil || !node.IsLogin() {
		return ErrRangeStatusErr
	}
	// 即使创建失败，raft leader添加成员
	var retry int = 3
	var err error
	for i := 0; i < retry; i++ {
		err = cluster.cli.CreateRange(node.GetServerAddr(), rng)
		if err == nil {
			break
		}
		// 链路问题
		if _, ok := err.(*client.RpcError); ok {
			break
		}
		if i+1 < retry {
			time.Sleep(time.Millisecond * time.Duration(10*(i+1)))
		}
	}
	return err
}
