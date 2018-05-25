package dskv

import (
	"model/pkg/kvrpcpb"
	"golang.org/x/net/context"
)

func (p *KvProxy) RawPut(req *kvrpcpb.KvRawPutRequest) (*kvrpcpb.KvRawPutResponse, error) {
	in := &Request{
		Type: Type_RawPut,
		RawPutReq: &kvrpcpb.DsKvRawPutRequest{
			Header: &kvrpcpb.RequestHeader{},
			Req:    req,
		},
	}
	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetKey())
	if err != nil {
		return nil, err
	}
	return resp.GetRawPutResp().GetResp(), nil
}

func (p *KvProxy) RawGet(req *kvrpcpb.KvRawGetRequest) (*kvrpcpb.KvRawGetResponse, error) {
	in := &Request{
		Type: Type_RawGet,
		RawGetReq: &kvrpcpb.DsKvRawGetRequest{
			Header: &kvrpcpb.RequestHeader{},
			Req:    req,
		},
	}
	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetKey())
	if err != nil {
		return nil, err
	}
	return resp.GetRawGetResp().GetResp(), nil
}

func (p *KvProxy) RawDelete(req *kvrpcpb.KvRawDeleteRequest) (*kvrpcpb.KvRawDeleteResponse, error) {
	in := &Request{
		Type: Type_RawDelete,
		RawDeleteReq: &kvrpcpb.DsKvRawDeleteRequest{
			Header: &kvrpcpb.RequestHeader{},
			Req:    req,
		},
	}
	bo := NewBackoffer(RawkvMaxBackoff, context.Background())
	resp, _, err := p.do(bo, in, req.GetKey())
	if err != nil {
		return nil, err
	}
	return resp.GetRawDeleteResp().GetResp(), nil
}
