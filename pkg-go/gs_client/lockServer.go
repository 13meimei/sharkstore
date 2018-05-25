package gs_client

import (
	"golang.org/x/net/context"
	"model/pkg/lockpb"
)

type LockServer struct {

}

func (service *LockServer) Lock(ctx context.Context, req *lockrpcpb.LockRequest) (*lockrpcpb.DLockResponse, error) {
	resp := service.handleLock(ctx, req)
	return resp, nil
}

func (service *LockServer) UnLock(ctx context.Context, req *lockrpcpb.UnLockRequest) (*lockrpcpb.DLockResponse, error) {
	resp := service.handleUnLock(ctx, req)
	return resp, nil
}

func (service *LockServer) ForceUnLock(ctx context.Context, req *lockrpcpb.ForceUnLockRequest) (*lockrpcpb.DLockResponse, error) {
	resp := service.handleForceUnLock(ctx, req)
	return resp, nil
}

func (service *LockServer) DoHeartbeat(ctx context.Context, req *lockrpcpb.LockHeartbeatRequest) (*lockrpcpb.DLockResponse, error) {
	resp := service.handleLockHeartbeat(ctx, req)
	return resp, nil
}