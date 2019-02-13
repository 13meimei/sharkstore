package server

import (
	"pkg-go/ds_client"
	"proxy/store/dskv"
	"model/pkg/txn"
)

func (p *Proxy) TxPrepare(context *dskv.ReqContext, t *Table) (*txnpb.PrepareResponse, error) {
	req := &txnpb.PrepareRequest{
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	resp, err := proxy.TxPrepare(nil, req, nil)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *Proxy) TxDecide(context *dskv.ReqContext, t *Table) (*txnpb.DecideResponse, error) {
	req := &txnpb.DecideRequest{
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	resp, err := proxy.TxDecide(nil, req, nil)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *Proxy) TxCleanup(context *dskv.ReqContext, t *Table) (*txnpb.ClearupResponse, error) {
	req := &txnpb.ClearupRequest{
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	resp, err := proxy.TxCleanup(nil, req, nil)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
