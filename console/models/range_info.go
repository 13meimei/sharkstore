package models


type RangeBrief struct {
	Id uint64 `json:"id,omitempty"`
	StartKey   string      `json:"start_key,omitempty"`
	EndKey     string      `json:"end_key,omitempty"`
	State      int32  `json:"state,omitempty"`
	LastHbTime string  `json:"last_hb_time,omitempty"`
	DownPeers      []uint64          `json:"down_peers,omitempty"`
	Peers      []uint64          `json:"peers,omitempty"`
	Leader      uint64          `json:"leader,omitempty"`
}

type PeerBrief struct {
	Id uint64 `json:"id,omitempty"`
	Index      uint64  `json:"index,omitempty"`
	Term 	   uint64  `json:"term,omitempty"`
	Commit     uint64          `json:"commit,omitempty"`
	StartKey   string      `json:"start_key,omitempty"`
	EndKey     string      `json:"end_key,omitempty"`
	NodeId      uint64          `json:"node_id,omitempty"`
	NodeAddress     string          `json:"node_address,omitempty"`
	nodeState     int32          `json:"node_state,omitempty"`
}
