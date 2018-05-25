package server

import (
	"golang.org/x/net/context"

	"model/pkg/lockpb"
	"model/pkg/kvrpcpb"
	"util/log"
	"util"
)

var (
	Lock_exist_error     			 string = "lock exist"
	Lock_not_exist_error		     string = "lock not exist"
	Lock_not_owner_error	         string = "not lock owner"
	Lock_force_unlock_error	         string = "force unlock ing"
	Lock_store_error	             string = "raft store error"
	Lock_epoch_error	             string = "range epoch error"
	Lock_namespace_no_exist	         string = "namespace not exist"
	Lock_network_error 				 string = "network exception"
	Lock_no_suport_force_unlock 	 string = "no support force unlock"
)

const (
	LOCK_OK = iota
	LOCK_EXIST_ERROR
	LOCK_NOT_EXIST_ERROR
	LOCK_NOT_OWNER_ERROR
	LOCK_FORCE_UNLOCK_ERROR
	LOCK_STORE_ERROR
	LOCK_EPOCH_ERROR
	LOCK_NAMESPACE_NO_EXIST
	LOCK_NETWORK_ERROR
	LOCK_NO_SUPPORT_FORCE_UNLOCK
)

var Lock_error_name = map[int64]string{
	LOCK_EXIST_ERROR: Lock_exist_error,
	LOCK_NOT_EXIST_ERROR: Lock_not_exist_error,
	LOCK_NOT_OWNER_ERROR: Lock_not_owner_error,
	LOCK_FORCE_UNLOCK_ERROR: Lock_force_unlock_error,
	LOCK_STORE_ERROR: Lock_store_error,
	LOCK_EPOCH_ERROR: Lock_epoch_error,
	LOCK_NAMESPACE_NO_EXIST: Lock_namespace_no_exist,
	LOCK_NETWORK_ERROR: Lock_network_error,
	LOCK_NO_SUPPORT_FORCE_UNLOCK: Lock_no_suport_force_unlock,
}

const dbName  = "lock"
func (service *Server) handleLock(ctx context.Context, req *lockrpcpb.LockRequest) (resp *lockrpcpb.DLockResponse) {
	log.Debug("recv client lock request, param:[%v]", req)
	dsResp, err :=  service.proxy.Lock(dbName, req.GetNamespace(), req.GetLockName(), req.GetConditions(), req.GetLockId(), req.GetTimeout(), util.GetIpFromContext(ctx))
	resp = getResponse("lock", dsResp, err, req.GetNamespace(), req.GetLockName())
	resp.Conditions = dsResp.GetValue()
	resp.UpdateTime = dsResp.GetUpdateTime()
	return
}

func getResponse(callType string, dsResp *kvrpcpb.LockResponse, err error, nameSpace, lockName string) *lockrpcpb.DLockResponse{
	if err != nil {
		log.Warn("client %s %s/%s handle failed, err: [%v]", callType, nameSpace, lockName, err)
		if err == ErrNotExistTable {
			return &lockrpcpb.DLockResponse{Code: LOCK_NAMESPACE_NO_EXIST, Error: Lock_namespace_no_exist}
		}
		return &lockrpcpb.DLockResponse{Code: LOCK_NETWORK_ERROR, Error: Lock_network_error}
	}

	if dsResp.GetCode() != 0  {
		log.Warn("client %s %s/%s handle failed, code:[%v], err: [%v]", callType, nameSpace, lockName, dsResp.GetCode(), dsResp.GetError())
		return &lockrpcpb.DLockResponse{Code: dsResp.GetCode(), Error: Lock_error_name[dsResp.GetCode()]}
	}
	return &lockrpcpb.DLockResponse{Code: LOCK_OK}
}

func (service *Server) handleUnLock(ctx context.Context, req *lockrpcpb.UnLockRequest) (resp *lockrpcpb.DLockResponse) {
	log.Debug("recv client unlock request, param:[%v]", req)
	dsResp, err :=  service.proxy.Unlock(dbName, req.GetNamespace(), req.GetLockName(), req.GetLockId(), util.GetIpFromContext(ctx))
	resp = getResponse("unlock", dsResp, err, req.GetNamespace(), req.GetLockName())
	return
}


func (service *Server) handleForceUnLock(ctx context.Context, req *lockrpcpb.ForceUnLockRequest) (resp *lockrpcpb.DLockResponse) {
	log.Debug("recv client force unlock request, param:[%v]", req)
	dsResp, err :=  service.proxy.UnlockForce(dbName, req.GetNamespace(), req.GetLockName(), util.GetIpFromContext(ctx))
	resp = getResponse("force unlock", dsResp, err, req.GetNamespace(), req.GetLockName())
	return
}


func (service *Server) handleLockHeartbeat(ctx context.Context, req *lockrpcpb.LockHeartbeatRequest) (resp *lockrpcpb.DLockResponse) {
	log.Debug("recv client lock heartbeat request, param:[%v]", req)
	dsResp, err :=  service.proxy.LockUpdate(dbName, req.GetNamespace(), req.GetLockName(), req.GetLockId(), []byte(""))
	resp = getResponse("hb", dsResp, err, req.GetNamespace(), req.GetLockName())
	return
}

func (service *Server) handleConditionUpdate(ctx context.Context, req *lockrpcpb.UpdateConditionRequest) (resp *lockrpcpb.DLockResponse) {
	log.Debug("recv client update lock condition request, param:[%v]", req)
	dsResp, err :=  service.proxy.UpdateCondition(dbName, req.GetNamespace(), req.GetLockName(), req.GetConditions())
	resp = getResponse("condition update", dsResp, err, req.GetNamespace(), req.GetLockName())
	return
}