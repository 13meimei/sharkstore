package server

import (
	"golang.org/x/net/context"

	"model/pkg/mspb"
	"model/pkg/taskpb"
	"util"
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
}

// Check check range heartbeat
func (c *RangeHeartChecker) Check(ctx context.Context, req *mspb.RangeHeartbeatRequest) *taskpb.Task {
	r := req.GetRange()
	log.Debug("[HB] range[%d:%d] heartbeat from ip[%s] Peers:%v DownPeers:%v", r.GetTableId(), r.GetId(),
		util.GetIpFromContext(ctx), req.GetRange().GetPeers(), req.GetDownPeers())

	return nil
}
