package server

import (
	"pkg-go/ds_client"
	"proxy/store/dskv"
	"model/pkg/txn"
)

func (p *Proxy) handlePrepare(ctx *dskv.ReqContext, req *txnpb.PrepareRequest, t *Table) (*txnpb.PrepareResponse, error) {
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	resp, err := proxy.TxPrepare(ctx, req, nil)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *Proxy) handleDecide(context *dskv.ReqContext, req *txnpb.DecideRequest, t *Table) (*txnpb.DecideResponse, error) {
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	resp, err := proxy.TxDecide(nil, req, nil)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *Proxy) handleCleanup(context *dskv.ReqContext, txId string, primaryKey []byte, t *Table) (*txnpb.ClearupResponse, error) {
	req := &txnpb.ClearupRequest{
		TxnId:      txId,
		PrimaryKey: primaryKey,
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
