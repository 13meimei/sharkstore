package server

import (
	"util/log"
	"strings"
)

/**
挑选合适的node
 */
type NodeSelector interface {
	Name() string
	CanSelect(node *Node) bool
}

type DifferIPSelector struct {
	ips []string
}

func NewDifferIPSelector(excludeNodes []*Node) *DifferIPSelector {
	ips := make([]string,0)
	for _, n := range excludeNodes {
		ip := strings.Split(n.GetServerAddr(), ":")[0]
		ips = append(ips,ip)
	}
	return &DifferIPSelector{
		ips: ips,
	}
}

func (sel *DifferIPSelector) Name() string {
	return "differ_ip"
}


func (sel *DifferIPSelector) CanSelect(node *Node) bool {
	nodeIp := strings.Split(node.GetServerAddr(), ":")[0]
	for _,ip := range sel.ips{
		if strings.Compare(ip,nodeIp) == 0 {
			return false
		}
	}
	return true
}

type DifferNodeSelector struct {
	excludeds map[uint64]struct{}
}

func newDifferNodeSelector(excludes map[uint64]struct{}) *DifferNodeSelector {
	return &DifferNodeSelector{
		excludeds:excludes,
	}
}

func (sel *DifferNodeSelector) Name() string {
	return "differ_node"
}

func (sel *DifferNodeSelector) CanSelect(node *Node) bool{
	_,ok := sel.excludeds[node.GetId()]
	return !ok
}


type DifferCacheNodeSelector struct {
	cache *idCache
}

func NewDifferCacheNodeSelector(cache *idCache) *DifferCacheNodeSelector {
	return &DifferCacheNodeSelector{cache: cache}
}

func (sel *DifferCacheNodeSelector) Name() string {
	return "differ_cachenode"
}

func (sel *DifferCacheNodeSelector) CanSelect(node *Node) bool {
	cached := sel.cache.get(node.GetId())
	return !cached
}


type NodeLoginSelector struct {
	opt *scheduleOption
}

func NewNodeLoginSelector(opt *scheduleOption) *NodeLoginSelector {
	return &NodeLoginSelector{opt: opt}
}

func (sel *NodeLoginSelector) Name() string {
	return "login"
}

func (sel *NodeLoginSelector) CanSelect(node *Node) bool {
	return node.IsLogin()
}

type SnapshotReceiveThresholdSelector struct {
	opt *scheduleOption
}

func NewSnapshotReceiveThresholdSelector(opt *scheduleOption) *SnapshotReceiveThresholdSelector {
	return &SnapshotReceiveThresholdSelector{opt: opt}
}

func (sel *SnapshotReceiveThresholdSelector) Name() string {
	return "receive-snapshot"
}

func (sel *SnapshotReceiveThresholdSelector) CanSelect(node *Node) bool {
	isBusy :=  maxUint64(uint64(node.GetReceivingSnapCount()), uint64(node.GetApplyingSnapCount())) > sel.opt.GetMaxSnapshotCount()
	if isBusy {
		log.Debug("snapshotReceiveThreshold cannot select node :%d, receive-snapshotCount: %d,%d, threshold: %d",
			node.GetId(), node.GetReceivingSnapCount(), node.GetApplyingSnapCount(), sel.opt.GetMaxSnapshotCount())
	}
	return !isBusy
}

// WriterOpsThresholdSelector ensures that we will not use an almost busy node as a target.
type WriterOpsThresholdSelector struct {
	opt *scheduleOption
}

func NewWriterOpsThresholdSelector(opt *scheduleOption) *WriterOpsThresholdSelector {
	return &WriterOpsThresholdSelector{opt: opt}
}

func (sel *WriterOpsThresholdSelector) Name() string {
	return "writeops"
}

func (sel *WriterOpsThresholdSelector) CanSelect(node *Node) bool {
	ok := node.opsStat.GetMax() < uint64(float64(sel.opt.GetWriteByteOpsThreshold())*float64(DefaultFactor))
	return ok
}


// storageThresholdFilter ensures that we will not use an almost full node as a target.
type StorageThresholdSelector struct {
	opt *scheduleOption
}

func NewStorageThresholdSelector(opt *scheduleOption) *StorageThresholdSelector {
	return &StorageThresholdSelector{opt: opt}
}

func (sel *StorageThresholdSelector) Name() string {
	return "storage-limit"
}

func (sel *StorageThresholdSelector) CanSelect(node *Node) bool {
	ok := node.availableRatio()*100  > float64(sel.opt.GetStorageAvailableThreshold()) / float64(DefaultFactor)
	return ok
}