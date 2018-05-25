package dskv

import (
	"model/pkg/kvrpcpb"
	"golang.org/x/net/context"
)

func (p *KvProxy) KvSet(req *kvrpcpb.KvSetRequest) (*kvrpcpb.KvSetResponse, error) {
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_KvSet
	in.KvSetReq = &kvrpcpb.DsKvSetRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}

	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetKv().GetKey())
	if err != nil {
		return nil, err
	}
	return resp.GetKvSetResp().GetResp(), nil
}

func (p *KvProxy) KvBatchSet(req *kvrpcpb.KvBatchSetRequest) (*kvrpcpb.KvBatchSetResponse, error) {
	if len(req.GetKvs()) == 0 {
		return &kvrpcpb.KvBatchSetResponse{}, nil
	}
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_KvBatchSet
	in.KvBatchSetReq =  &kvrpcpb.DsKvBatchSetRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}

	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetKvs()[0].GetKey())
	if err != nil {
		return nil, err
	}
	return resp.GetKvBatchSetResp().GetResp(), nil
}

func (p *KvProxy) KvGet(req *kvrpcpb.KvGetRequest) (*kvrpcpb.KvGetResponse, error) {
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_KvGet
	in.KvGetReq = &kvrpcpb.DsKvGetRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}

	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetKey())
	if err != nil {
		return nil, err
	}
	return resp.GetKvGetResp().GetResp(), nil
}

func (p *KvProxy) KvBatchGet(req *kvrpcpb.KvBatchGetRequest) (*kvrpcpb.KvBatchGetResponse, error) {
	if len(req.GetKeys()) == 0 {
		return &kvrpcpb.KvBatchGetResponse{}, nil
	}
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_KvBatchGet
	in.KvBatchGetReq = &kvrpcpb.DsKvBatchGetRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}

	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetKeys()[0])
	if err != nil {
		return nil, err
	}
	return resp.GetKvBatchGetResp().GetResp(), nil
}

func (p *KvProxy) KvScan(req *kvrpcpb.KvScanRequest) (*kvrpcpb.KvScanResponse, error) {
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_KvScan
	in.KvScanReq = &kvrpcpb.DsKvScanRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}

	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetStart())
	if err != nil {
		return nil, err
	}
	return resp.GetKvScanResp().GetResp(), nil
}

func (p *KvProxy) KvDelete(req *kvrpcpb.KvDeleteRequest) (*kvrpcpb.KvDeleteResponse, error) {
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_KvDelete
	in.KvDeleteReq = &kvrpcpb.DsKvDeleteRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}
	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetKey())
	if err != nil {
		return nil, err
	}
	return resp.GetKvDeleteResp().GetResp(), nil
}

func (p *KvProxy) KvBatchDelete(req *kvrpcpb.KvBatchDeleteRequest) (*kvrpcpb.KvBatchDeleteResponse, error) {
	if len(req.GetKeys()) == 0 {
		return &kvrpcpb.KvBatchDeleteResponse{}, nil
	}
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_KvBatchDel
	in.KvBatchDelReq = &kvrpcpb.DsKvBatchDeleteRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}
	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetKeys()[0])
	if err != nil {
		return nil, err
	}
	return resp.GetKvBatchDelResp().GetResp(), nil
}

func (p *KvProxy) KvRangeDelete(req *kvrpcpb.KvRangeDeleteRequest) (*kvrpcpb.KvRangeDeleteResponse, error) {
	in := GetRequest()
	defer PutRequest(in)
	in.Type = Type_KvRangeDel
	in.KvRangeDelReq =  &kvrpcpb.DsKvRangeDeleteRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:    req,
	}
	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetStart())
	if err != nil {
		return nil, err
	}
	return resp.GetKvRangeDelResp().GetResp(), nil
}

