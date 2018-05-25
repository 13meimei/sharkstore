package server

import (
	"golang.org/x/net/context"
	"model/pkg/lockpb"
)



func (service *Server) Lock(ctx context.Context, req *lockrpcpb.LockRequest) (*lockrpcpb.DLockResponse, error) {
	resp := service.handleLock(ctx, req)
	return resp, nil
}

func (service *Server) UnLock(ctx context.Context, req *lockrpcpb.UnLockRequest) (*lockrpcpb.DLockResponse, error) {
	resp := service.handleUnLock(ctx, req)
	return resp, nil
}

func (service *Server) ForceUnLock(ctx context.Context, req *lockrpcpb.ForceUnLockRequest) (*lockrpcpb.DLockResponse, error) {
	resp := service.handleForceUnLock(ctx, req)
	return resp, nil
}

func (service *Server) DoHeartbeat(ctx context.Context, req *lockrpcpb.LockHeartbeatRequest) (*lockrpcpb.DLockResponse, error) {
	resp := service.handleLockHeartbeat(ctx, req)
	return resp, nil
}

func (service *Server) UpdateCondition(ctx context.Context, req *lockrpcpb.UpdateConditionRequest) (*lockrpcpb.DLockResponse, error){
	resp := service.handleConditionUpdate(ctx, req)
	return resp, nil
}