package server

import (
	"model/pkg/kvrpcpb"
	"pkg-go/ds_client"
	"proxy/store/dskv"
)

// TODO timestamp to ensure the order of put
func (p *Proxy) RawPut(dbName, tableName string, key, value []byte) error {
	t := p.router.FindTable(dbName, tableName)
	if t == nil {
		return ErrNotExistTable
	}
	req := &kvrpcpb.KvRawPutRequest{
		Key:   key,
		Value: value,
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	_, err := proxy.RawPut(req)
	if err != nil {
		return err
	}
	return nil
}

// get in range split, first get from dst range, if no value, we need get from src range again
func (p *Proxy) RawGet(dbName, tableName string, key []byte) ([]byte, error) {
	t := p.router.FindTable(dbName, tableName)
	if t == nil {
		return nil, ErrNotExistTable
	}
	req := &kvrpcpb.KvRawGetRequest{
		Key: key,
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	resp, err := proxy.RawGet(req)
	if err != nil {
		return nil, err
	}

	//code := resp.GetKvRawGetResp().GetCode()
	value := resp.GetValue()
	return value, nil

}

// delete in range split, we need delete key in src range and dst range
func (p *Proxy) RawDelete(dbName, tableName string, key []byte) error {
	t := p.router.FindTable(dbName, tableName)
	if t == nil {
		return ErrNotExistTable
	}
	req := &kvrpcpb.KvRawDeleteRequest{
		Key: key,
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	_, err := proxy.RawDelete(req)
	if err != nil {
		return err
	}
	return nil
}
