package server

import (
	"fmt"
	"time"

	"model/pkg/metapb"
	"model/pkg/mspb"
	"util/log"
)

// Update update range's states from range heartbeat
// presist: if we should persist range states to storage
func (r *Range) Update(hb *mspb.RangeHeartbeatRequest) (presist bool, err error) {
	// invalid request or range id mismatched
	if req == nil || hb.GetRange() == nil || hb.GetRange().GetId() != r.id || hb.GetRange().GetRangeEpoch() == nil {
		return false, fmt.Errorf("invalid heartbeat for update range %d: %v", r.id, hb)
	}

	// do update
	r.Lock()
	defer r.Unlock()

	// check if heartbeat is stale
	err = r.maybeStale(hb)
	if err != nil {
		return false, err
	}

	// try to update term
	if hb.GetTerm() > r.GetTerm() {
		log.Info("range[%d]: term %d->%d", r.id, r.term, hb.GetTerm())
		r.setTerm(hb.Term())
	}

	// try to update meta
	var metaUpdated bool
	metaUpdated, err = r.maybeMetaUpdate(hb.GetRange())
	if err != nil {
		return
	}
	if metaUpdated {
		r.meta = hb.GetRange()
		presist = true
	}

	// try to update leader
	err = r.updateLeader(hb.GetLeader())
	if err != nil {
		return
	}

	r.downPeers = hb.GetDownPeers()
	r.progresses = hb.GetProgresses()

	// updat stats
	stat := hb.GetStats()
	if stat != nil {
		r.BytesRead = stat.BytesRead
		r.BytesWritten = stat.BytesWritten
		r.KeysRead = stat.KeysRead
		r.KeysWritten = stat.KeysWritten
		r.ApproximateSize = stat.ApproximateSize
		r.opsStat.Hit(stat.BytesWritten)
	}

	r.lastHbTimeTS = time.Now()

	return
}

// unlocked
func (r *Range) maybeStale(hb *mspb.RangeHeartbeatRequest) error {
	if hb.GetTerm() < r.GetTerm() {
		return fmt.Errorf("low term: %d, current: %d", hb.GetTerm(), r.GetTerm())
	}

	hbEpoch := hb.GetRange().GetRangeEpoch()
	if hbEpoch.GetVersion() < r.meta.GetRangeEpoch().GetVersion() {
		return fmt.Errorf("low range version: %d, current: %d", hbEpoch.GetVersion(),
			r.meta.GetRangeEpoch().GetVersion())
	}

	if hbEpoch.GetConfVer() < r.meta.GetRangeEpoch().GetConfVer() {
		return fmt.Errorf("low conf version: %d, current: %d", hbEpoch.GetConfVer(),
			r.meta.GetRangeEpoch().GetConfVer())
	}
	return nil
}

// prerequisite: no version changed
// unlocked
func (r *Range) maybePromoted(meta *metapb.Range) (bool, error) {
	for _, newPeer := range meta.GetPeers() {
		found := false
		for _, oldPeer := range r.meta.GetPeers() {
			if oldPeer.GetId() == newPeer.GetId() {
				found = true
				if oldPeer.GetType() != newPeer.GetType() {
					return true, nil
				}
			}
		}
		if !found {
			return false, fmt.Errorf("inconsistent peers when check promoted, old:[%v], new:[%v]",
				r.meta.GetPeers(), meta.GetPeers())
		}
	}
	return false, nil
}

// unlocked
func (r *Range) maybeMetaUpdate(meta *metapb.Range) (bool, error) {
	oldVerion := r.meta.GetRangeEpoch().GetVersion()
	oldConfVer := r.meta.GetRangeEpoch().GetConfVer()

	update := meta.GetRangeEpoch().GetVersion() > oldVerion ||
		meta.GetRangeEpoch().GetConfVer() > oldConfVer
	if update {
		log.Info("range[%d] version: %d->%d, confver: %d->%d", r.id,
			oldVerion, meta.GetRangeEpoch().GetVersion(), oldConfVer, meta.GetRangeEpoch().GetConfVer())
		return true, nil
	}

	// check learner promoted
	return r.maybePromoted(meta)
}

// unlocked
func (r *Range) updateLeader(newLeader uint64) error {
	for _, peer := range r.meta.GetPeers() {
		if peer.GetId() == newLeader {
			r.leader = peer
			return nil
		}
	}
	return fmt.Errorf("could find leader in peers to update. leader: %d, peers: %v",
		newLeader, r.meta.GetPeers())
}
