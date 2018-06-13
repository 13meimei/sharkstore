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
	_ = totalVoters
	_ = downVoters
	return false
}

func (c *RangeHeartChecker) validate(r *Range) bool {
	if r == nil {
		return false
	}
	return true
}

func (c *RangeHeartChecker) needToStore(r *Range) bool {
	return true
}

// Check check range heartbeat
func (c *RangeHeartChecker) Check(ctx context.Context, req *mspb.RangeHeartbeatRequest) *taskpb.Task {
	from := util.GetIpFromContext(ctx)
	r := req.GetRange()
	if r == nil {
		log.Warn("[HB] invalid range: <nil> from %s".from)
		return nil
	}

	log.Debug("[HB] range[%d:%d] heartbeat from ip[%s] Peers:%v DownPeers:%v", r.GetTableId(), r.GetId(),
		from, req.GetRange().GetPeers(), req.GetDownPeers())
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
