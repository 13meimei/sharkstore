package dskv

import (
	"bytes"
	"time"

	"proxy/metric"
	"util/log"
	"model/pkg/kvrpcpb"
	"model/pkg/txn"
)

//func (p *KvProxy) SqlInsert(req *kvrpcpb.InsertRequest, scope *kvrpcpb.Scope) ([]*kvrpcpb.InsertResponse, error) {
//	//var key, start, limit []byte
//	var resp *kvrpcpb.InsertResponse
//	var resps []*kvrpcpb.InsertResponse
//	//var route *KeyLocation
//	var err error
//
//	start := scope.Start
//	//limit = scope.Limit
//	//for {
//	/*	if key == nil {
//			key = start
//		} else if route != nil {
//			key = route.EndKey
//			// check key in range
//			if bytes.Compare(key, start) < 0 || bytes.Compare(key, limit) >= 0 {
//				// 遍历完成，直接退出循环
//				break
//			}
//		} else {
//			// must bug
//			log.Error("invalid route, must bug!!!!!!!")
//			return nil, ErrInternalError
//		}*/
//		log.Debug("insert request start key:%v",start)
//		resp, _, err = p.Insert(req, start)
//		if err != nil {
//			return nil, err
//		}
//		resps = append(resps, resp)
//	//}
//	if len(resps) == 0 {
//		log.Warn("SqlInsert: should not enter into here")
//		resp = &kvrpcpb.InsertResponse{Code: 0, AffectedKeys: 0}
//		resps = append(resps, resp)
//	}
//	return resps, nil
//}

func (p *KvProxy) Insert(rContext *ReqContext, req *kvrpcpb.InsertRequest, key []byte) (*kvrpcpb.InsertResponse, *KeyLocation, error) {
	startTime := time.Now()
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_Insert
	in.InsertReq = &kvrpcpb.DsInsertRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}
	resp, l, err := p.do(rContext.GetBackOff(), in, key)
	delay := time.Now().Sub(startTime)
	if err != nil {
		metric.GsMetric.StoreApiMetric("KvInsert", false, delay)
	} else {
		metric.GsMetric.StoreApiMetric("KvInsert", true, delay)
	}
	if err != nil {
		return nil, nil, err
	}
	response := resp.GetInsertResp().GetResp()
	if response != nil && response.GetCode() == 0 && response.GetAffectedKeys() != uint64(len(req.Rows)) {
		var nodeId uint64 = 0
		l, err = p.RangeCache.LocateKey(rContext.GetBackOff(), key)
		if l != nil {
			nodeId = l.NodeId
		}
		log.Error("[insert]nodeId:%d,request:[%v],response exception, response:[%v] ", nodeId, req, response)
		return nil, nil, ErrAffectRows
	}
	if response == nil || response.GetCode() > 0 {
		var nodeId uint64 = 0
		l, err = p.RangeCache.LocateKey(rContext.GetBackOff(), key)
		if l != nil {
			nodeId = l.NodeId
		}
		log.Error("[insert]nodeId:%d,request:[%v],response exception, response:[%v] ", nodeId, req, response)
		return response, nil, ErrInternalError
	}
	return response, l, nil
}

func (p *KvProxy) Update(rContext *ReqContext, req *kvrpcpb.UpdateRequest, key []byte) (*kvrpcpb.UpdateResponse, error) {
	startTime := time.Now()
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_Update
	in.UpdateReq = &kvrpcpb.DsUpdateRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}
	resp, l, err := p.do(rContext.GetBackOff(), in, key)
	delay := time.Now().Sub(startTime)
	if err != nil {
		metric.GsMetric.StoreApiMetric("KvUpdate", false, delay)
	} else {
		metric.GsMetric.StoreApiMetric("KvUpdate", true, delay)
	}
	if err != nil {
		return nil, err
	}
	response := resp.GetUpdateResp().GetResp()
	if response == nil || response.GetCode() > 0 {
		var nodeId uint64 = 0
		l, err = p.RangeCache.LocateKey(rContext.GetBackOff(), key)
		if l != nil {
			nodeId = l.NodeId
		}
		log.Error("[update]nodeId:%d,request:[%v],response exception, response:[%v] ", nodeId, req, response)
		return response, ErrInternalError
	}
	return response, nil
}

func (p *KvProxy) SqlQuery(rContext *ReqContext, req *txnpb.SelectRequest, key []byte) (*txnpb.SelectResponse, *KeyLocation, error) {
	log.Debug("select by route key: %v", key)
	var err, errForRetry error
	for {
		if errForRetry != nil {
			errForRetry = rContext.GetBackOff().Backoff(BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("[select]%s execute timeout", rContext)
				break
			}
		}
		startTime := time.Now()
		in := GetRequest()
		in.Type = Type_Select
		in.TxSelectReq = &txnpb.DsSelectRequest{
			Header: &kvrpcpb.RequestHeader{},
			Req:    req,
		}
		var (
			resp *Response
			l    *KeyLocation
		)
		resp, l, err = p.do(rContext.GetBackOff(), in, key)
		delay := time.Now().Sub(startTime)
		if err != nil {
			metric.GsMetric.StoreApiMetric("KvQuery", false, delay)
		} else {
			metric.GsMetric.StoreApiMetric("KvQuery", true, delay)
		}
		PutRequest(in)
		if err != nil {
			if err == ErrRouteChange {
				errForRetry = err
				continue
			}
			break
		}
		return resp.GetTxSelectResp().GetResp(), l, nil
	}
	return nil, nil, err
}

func (p *KvProxy) SqlDelete(req *kvrpcpb.DeleteRequest, scope *kvrpcpb.Scope) ([]*kvrpcpb.DeleteResponse, error) {
	var key, start, limit []byte
	var resp *kvrpcpb.DeleteResponse
	var resps []*kvrpcpb.DeleteResponse
	var route *KeyLocation
	var err error

	start = scope.Start
	limit = scope.Limit
	for {
		if key == nil {
			key = start
		} else if route != nil {
			key = route.EndKey
			// check key in range
			if bytes.Compare(key, start) < 0 || bytes.Compare(key, limit) >= 0 {
				// 遍历完成，直接退出循环
				break
			}
		} else {
			return nil, ErrInternalError
		}
		resp, route, err = p.Delete(req, key)
		if err != nil {
			return nil, err
		}
		resps = append(resps, resp)
	}
	if len(resps) == 0 {
		resp = &kvrpcpb.DeleteResponse{Code: 0, AffectedKeys: 0}
		resps = append(resps, resp)
	}
	return resps, nil
}

func (p *KvProxy) Delete(req *kvrpcpb.DeleteRequest, key []byte) (*kvrpcpb.DeleteResponse, *KeyLocation, error) {
	var retErr, errForRetry error
	context := NewPRConext(ScannerNextMaxBackoff)
	for {
		if errForRetry != nil {
			errForRetry = context.GetBackOff().Backoff(BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("%s execute timeout", context)
				break
			}
		}
		start := time.Now()
		in := GetRequest()
		defer PutRequest(in)
		in.Type = Type_Delete
		in.DeleteReq = &kvrpcpb.DsDeleteRequest{
			Header: &kvrpcpb.RequestHeader{},
			Req:    req,
		}
		resp, l, err := p.do(context.GetBackOff(), in, key)
		delay := time.Now().Sub(start)
		if err != nil {
			metric.GsMetric.StoreApiMetric("KvDelete", false, delay)
		} else {
			metric.GsMetric.StoreApiMetric("KvDelete", true, delay)
		}
		if err != nil {
			if err == ErrRouteChange {
				log.Info("delete failure ,route change key:%v", key)
				retErr = err
				errForRetry = err
				continue
			}
			return nil, nil, err
		}
		return resp.GetDeleteResp().GetResp(), l, nil
	}
	return nil, nil, retErr
}

type KvParisSlice []*kvrpcpb.KeyValue

func (p KvParisSlice) Len() int {
	return len(p)
}

func (p KvParisSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p KvParisSlice) Less(i int, j int) bool {
	return bytes.Compare(p[i].GetKey(), p[j].GetKey()) < 0
}

func (p *KvProxy) TxPrepare(rContext *ReqContext, req *txnpb.PrepareRequest, key []byte) (*txnpb.PrepareResponse, error) {
	startTime := time.Now()
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_TxPrepare
	in.TxPrepareReq = &txnpb.DsPrepareRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}
	resp, _, err := p.do(rContext.GetBackOff(), in, key)
	delay := time.Now().Sub(startTime)
	if err != nil {
		metric.GsMetric.StoreApiMetric("TxPrepare", false, delay)
	} else {
		metric.GsMetric.StoreApiMetric("TxPrepare", true, delay)
	}
	if err != nil {
		return nil, err
	}
	response := resp.GetTxPrepareResp().GetResp()
	return response, nil
}
func (p *KvProxy) TxDecide(rContext *ReqContext, req *txnpb.DecideRequest, key []byte) (*txnpb.DecideResponse, error) {
	startTime := time.Now()
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_TxDecide
	in.TxDecideReq = &txnpb.DsDecideRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}
	resp, _, err := p.do(rContext.GetBackOff(), in, key)
	delay := time.Now().Sub(startTime)
	if err != nil {
		metric.GsMetric.StoreApiMetric("TxDecide", false, delay)
	} else {
		metric.GsMetric.StoreApiMetric("TxDecide", true, delay)
	}
	if err != nil {
		return nil, err
	}
	response := resp.GetTxDecideResp().GetResp()
	return response, nil
}

func (p *KvProxy) TxCleanup(rContext *ReqContext, req *txnpb.ClearupRequest, key []byte) (*txnpb.ClearupResponse, error) {
	startTime := time.Now()
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_TxCleanup
	in.TxCleanupReq = &txnpb.DsClearupRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}
	resp, _, err := p.do(rContext.GetBackOff(), in, key)
	delay := time.Now().Sub(startTime)
	if err != nil {
		metric.GsMetric.StoreApiMetric("TxCleanup", false, delay)
	} else {
		metric.GsMetric.StoreApiMetric("TxCleanup", true, delay)
	}
	if err != nil {
		return nil, err
	}
	response := resp.GetTxCleanupResp().GetResp()
	return response, nil
}

func (p *KvProxy) TxGetLock(rContext *ReqContext, req *txnpb.GetLockInfoRequest, key []byte) (*txnpb.GetLockInfoResponse, error) {
	startTime := time.Now()
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_TxGetLock
	in.TxGetLockReq = &txnpb.DsGetLockInfoRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}
	resp, _, err := p.do(rContext.GetBackOff(), in, key)
	delay := time.Now().Sub(startTime)
	if err != nil {
		metric.GsMetric.StoreApiMetric("TxGetLock", false, delay)
	} else {
		metric.GsMetric.StoreApiMetric("TxGetLock", true, delay)
	}
	if err != nil {
		return nil, err
	}
	response := resp.GetTxLockInfoResp().GetResp()
	return response, nil
}

func (p *KvProxy) TxScan(rContext *ReqContext, req *txnpb.ScanRequest, key []byte) (*txnpb.ScanResponse, error) {
	startTime := time.Now()
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_TxScan
	in.TxScanReq = &txnpb.DsScanRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}
	resp, _, err := p.do(rContext.GetBackOff(), in, key)
	delay := time.Now().Sub(startTime)
	if err != nil {
		metric.GsMetric.StoreApiMetric("ScanKeyValue", false, delay)
	} else {
		metric.GsMetric.StoreApiMetric("ScanKeyValue", true, delay)
	}
	if err != nil {
		return nil, err
	}
	response := resp.GetTxScanResp().GetResp()
	return response, nil
}
