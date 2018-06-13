package server

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"model/pkg/metapb"
	"model/pkg/mspb"
)

// Range range's data and states
type Range struct {
	sync.RWMutex

	id      uint64 // const range id
	tableID uint64 // const table id

	meta       *metapb.Range // keep not nil
	leader     *metapb.Peer  // leader peer id
	term       uint64        // newest term
	downPeers  []*mspb.DownPeer
	progresses []*mspb.ReplicateProgress

	BytesWritten uint64
	BytesRead    uint64

	KeysWritten uint64
	KeysRead    uint64
	opsStat     RangeOpsStat
	// Approximate range size.
	ApproximateSize uint64

	state metapb.RangeState
	Trace bool

	lastHbTimeTS time.Time
}

// NewRange create a new range
func NewRange(meta *metapb.Range) *Range {
	return &Range{
		id:      meta.GetId(),
		tableID: meta.GetTableId(),
		meta:    meta,
	}
}

// GetTableID return range's talbe id
func (r *Range) GetTableID() uint64 {
	return r.tableID
}

// GetID return range id
func (r *Range) GetID() uint64 {
	return r.ID
}

func (r *Range) setTerm(term uint64) {
	atomic.StoreUint64(&r.term, term)
}

// GetTerm return range's term
func (r *Range) GetTerm() (term uint64) {
	atomic.LoadUint64(r.term)
}

// GetLeader return current leader
func (r *Range) GetLeader() (leader *metapb.Peer) {
	r.RLock()
	leader = r.leader
	r.RUnlock()
}

// GetVersion return range version
func (r *Range) GetVersion() (ver uint64) {
	r.RLock()
	ver = r.meta.GetRangeEpoch().GetVersion()
	r.RUnlock()
}

// GetConfVer return conf verion
func (r *Range) GetConfVer() (ver uint64) {
	r.RLock()
	ver = r.meta.GetRangeEpoch().GetConfVer()
	r.RUnlock()
}

// GetMeta return range meta
func (r *Range) GetMeta() (meta *metapb.Range) {
	r.RLock()
	meta = r.meta
	r.RUnlock()
}

// SString to printable string
func (r *Range) SString() string {
	return fmt.Sprintf("%d:%d", r.tableID, r.ID)
}

func (r *Range) getPeerUnlock(peerID uint64) *metapb.Peer {
	for _, peer := range r.meta.GetPeers() {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetPeer return the peer with specified peer id
func (r *Range) GetPeer(peerID uint64) *metapb.Peer {
	r.RLock()
	defer r.RUnlock()

	return r.getPeerUnlock(peerID)
}

// GetPeers return peers
func (r *Range) GetPeers() []*metapb.Peer {
	r.RLock()
	defer r.RUnlock()

	return r.meta.GetPeers()
}

// GetDownPeer return the down peers with specified peer id
func (r *Range) GetDownPeer(peerID uint64) *metapb.Peer {
	r.RLock()
	defer r.RUnlock()

	for _, down := range r.DownPeers {
		if down.GetPeer().GetId() == peerID {
			return r.getPeerUnlock(peerID)
		}
	}
	return nil
}

// GetDownPeers return down peers
func (r *Range) GetDownPeers() (peers []*metapb.Peer) {
	r.RLock()
	for _, down := range r.downPeers {
		peer := r.getPeerUnlock(down.GetPeerId())
		if peer != nil {
			peers = append(peers, peer)
		}
	}
	r.RUnlock()
}

// GetNodePeer return the peer in specified Node
func (r *Range) GetNodePeer(nodeID uint64) *metapb.Peer {
	r.RLock()
	defer r.RUnlock()

	for _, peer := range r.GetPeers() {
		if peer.GetNodeId() == nodeID {
			return peer
		}
	}
	return nil
}

// GetNodes return nodes
func (r *Range) GetNodes(cluster *Cluster) (nodes []*Node) {
	nodeIDs := r.GetNodeIDs()
	for nodeID := range nodeIDs {
		node := cluster.FindNodeById(peer.GetNodeId())
		if node == nil {
			continue
		}
		nodes = append(nodes, node)
	}
	return
}

// GetNodeIDs return a map indicate the region distributed
func (r *Range) GetNodeIDs() map[uint64]struct{} {
	nodes := make(map[uint64]struct{})

	r.RLock()
	defer r.RUnlock()

	for _, peer := range r.meta.Peers {
		nodes[peer.GetNodeId()] = struct{}{}
	}
	return nodes
}

// GetFollowers return a map indicate the follow peers distributed
func (r *Range) GetFollowers() map[uint64]*metapb.Peer {
	followers := make(map[uint64]*metapb.Peer)

	r.RLock()
	defer r.RUnlock()

	for _, peer := range r.meta.Peers {
		if r.leader != nil && r.leader.GetId() == peer.GetId() {
			continue
		}
		followers[peer.GetNodeId()] = peer
	}
	return followers
}

// LastHeartbeat return last heartbeat time
func (r *Range) LastHeartbeat() (t *time.Time) {
	r.RLock()
	t = r.lastHbTimeTS
	r.RUnlock()
}

// GetState return state
func (r *Range) GetState() metapb.RangeState {
	return atomic.LoadInt32(&r.state)
}

// SetState set state
func (r *Range) SetState(state metapb.RangeState) {
	atomic.StoreInt32(&r.state, state)
}

// IsHealthy return true if range is healthy
func (r *Range) IsHealthy() bool {
	r.RLock()
	defer r.RUnlock()

	return len(r.downPeers) > 0 ||
		r.GetState() == metapb.RangeState_R_Remove
}
