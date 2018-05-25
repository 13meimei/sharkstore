package util

import (
	"sync"

	"model/pkg/raft_cmdpb"
)

var raftCmdReqPool *sync.Pool = &sync.Pool{
	New:func() interface {}{
		return &raft_cmdpb.RaftCmdRequest{}
	},
}

var raftCmdRespPool *sync.Pool = &sync.Pool{
	New:func() interface {}{
		return &raft_cmdpb.RaftCmdResponse{}
	},
}

var raftReqPool *sync.Pool = &sync.Pool{
	New:func() interface {}{
		return &raft_cmdpb.Request{}
	},
}

func GetRaftCmdReq() *raft_cmdpb.RaftCmdRequest {
	return raftCmdReqPool.Get().(*raft_cmdpb.RaftCmdRequest)
}

func PutRaftCmdReq(req *raft_cmdpb.RaftCmdRequest){
	if req == nil {
		return
	}
	req.Reset()
	raftCmdReqPool.Put(req)
}

func GetRaftCmdResp() *raft_cmdpb.RaftCmdResponse {
	return raftCmdRespPool.Get().(*raft_cmdpb.RaftCmdResponse)
}

func PutRaftCmdResp(resp *raft_cmdpb.RaftCmdResponse){
	if resp == nil {
		return
	}
	resp.Reset()
	raftCmdRespPool.Put(resp)
}



func GetRaftReq() *raft_cmdpb.Request {
	return raftReqPool.Get().(*raft_cmdpb.Request)
}

func PutRaftReq(req *raft_cmdpb.Request){
	if req == nil {
		return
	}
	req.Reset()
	raftReqPool.Put(req)
}