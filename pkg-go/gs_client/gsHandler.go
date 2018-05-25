package gs_client

import (
	"golang.org/x/net/context"

	"model/pkg/lockpb"
	"util/log"
	"time"
)

func (service *LockServer) handleLock(ctx context.Context, req *lockrpcpb.LockRequest) (resp *lockrpcpb.DLockResponse) {
	log.Debug("get client lock request, param:[%v]", req)
	return &lockrpcpb.DLockResponse{UpdateTime:time.Now().Unix()}
}


func (service *LockServer) handleUnLock(ctx context.Context, req *lockrpcpb.UnLockRequest) (resp *lockrpcpb.DLockResponse) {
	log.Debug("get client unlock request, param:[%v]", req)
	return &lockrpcpb.DLockResponse{UpdateTime:time.Now().Unix()}
}


func (service *LockServer) handleForceUnLock(ctx context.Context, req *lockrpcpb.ForceUnLockRequest) (resp *lockrpcpb.DLockResponse) {
	log.Debug("get client force unlock request, param:[%v]", req)
	return &lockrpcpb.DLockResponse{UpdateTime:time.Now().Unix()}
}


func (service *LockServer) handleLockHeartbeat(ctx context.Context, req *lockrpcpb.LockHeartbeatRequest) (resp *lockrpcpb.DLockResponse) {
	log.Debug("get client lock heartbeat request, param:[%v]", req)
	return &lockrpcpb.DLockResponse{UpdateTime:time.Now().Unix()}
}

