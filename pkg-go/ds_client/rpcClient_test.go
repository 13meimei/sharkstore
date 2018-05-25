package client

import (
	"fmt"
	"model/pkg/kvrpcpb"
	"model/pkg/timestamp"
	"net"
	"testing"
	"time"

	"sync"
	"pkg-go/util"
	"model/pkg/funcpb"
)

var svr *RpcServer

func init() {
	svr = NewRpcServer("127.0.0.1:6180")
	go svr.Start()
	time.Sleep(time.Second)
	fmt.Println("init client IP +127.0.0.1:6180")
}

func TestDSRpcClient_KvRawGet(t *testing.T) {
	cli := NewRPCClient()
	in := &kvrpcpb.DsKvRawGetRequest{
		Header: initHeader(),
		Req:    &kvrpcpb.KvRawGetRequest{Key: []byte("abcd")},
	}
	out, err := cli.RawGet("127.0.0.1:6180", in, WriteTimeout, ReadTimeoutShort)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		return
	}
	if string(out.GetResp().GetValue()) != "ok" {
		t.Error("test failed")
		return
	}
}

func TestDSRpcClient_KvRawGet1(t *testing.T) {
	cli := NewRPCClient()
	in := &kvrpcpb.DsKvRawGetRequest{
		Header: initHeader(),
		Req:    &kvrpcpb.KvRawGetRequest{Key: []byte("abcd")},
	}
	//out, err := cli.RawGet("127.0.0.1:6180", in, WriteTimeout, ReadTimeoutShort)
	//if err != nil {
	//	t.Errorf("test failed, err[%v]", err)
	//	return
	//}
	//if string(out.GetResp().GetValue()) != "ok" {
	//	t.Error("test failed")
	//	return
	//}
	svr.Stop()
	fmt.Println("svr stop !!!!!")
	out, err := cli.RawGet("127.0.0.1:6180", in, WriteTimeout, ReadTimeoutShort)
	if err == nil {
		t.Errorf("test failed, out[%v]", out)
		time.Sleep(time.Second)
		return
	}
	time.Sleep(time.Second)
	fmt.Println("retry raw get!!!!")
	out, err = cli.RawGet("127.0.0.1:6180", in, WriteTimeout, ReadTimeoutShort)
	if err == nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr = NewRpcServer("127.0.0.1:6180")
	fmt.Println("svr restart !!!!!!")
	go svr.Start()
	time.Sleep(time.Second)
	out, err = cli.RawGet("127.0.0.1:6180", in, WriteTimeout, ReadTimeoutShort)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	if string(out.GetResp().GetValue()) != "ok" {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
}

func TestDSRpcClient_KvRawGet2(t *testing.T) {
	cli := NewRPCClient()
	in := &kvrpcpb.DsKvRawGetRequest{
		Header: initHeader(),
		Req:    &kvrpcpb.KvRawGetRequest{Key: []byte("abcd")},
	}
	_, err := cli.RawGet("127.0.0.1:6180", in, WriteTimeout, 0)
	if err == nil {
		t.Errorf("test failed, err[%v]", err)
		return
	}
}

func initHeader() *kvrpcpb.RequestHeader {
	return &kvrpcpb.RequestHeader{
		ClusterId: 1,
		Timestamp: &timestamp.Timestamp{
			WallTime: 1542222222,
			Logical:  1,
		},
		TraceId: 1,
		RangeId: 1,
	}
}

func TestDSRpcClient_KvRawPut(t *testing.T) {
	cli := NewRPCClient()
	in := &kvrpcpb.DsKvRawPutRequest{
		Header: initHeader(),
		Req: &kvrpcpb.KvRawPutRequest{
			Key:   []byte("abcd"),
			Value: []byte("abcd-value"),
		},
	}
	_, err := cli.RawPut("127.0.0.1:6180", in, WriteTimeout, time.Second)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		return
	}
}

type RpcServer struct {
	rpcAddr     string
	wg          sync.WaitGroup
	lock        sync.RWMutex
	count       uint64
	conns       map[uint64]net.Conn
	l net.Listener
}

func NewRpcServer(addr string) *RpcServer {
	return &RpcServer{rpcAddr: addr, conns: make(map[uint64]net.Conn)}
}

func (svr *RpcServer) Start() {
	l, err := net.Listen("tcp", svr.rpcAddr)
	if err != nil {
		return
	}
	svr.l = l
	svr.wg.Add(1)
	// A common pattern is to start a loop to continously accept connections
	for {
		//accept connections using Listener.Accept()
		c, err := l.Accept()
		if err != nil {
			svr.wg.Done()
			return
		}
		//It's common to handle accepted connection on different goroutines
		svr.wg.Add(1)
		svr.count++
		svr.lock.Lock()
		svr.conns[svr.count] = c
		svr.lock.Unlock()
		fmt.Println("===============new connect!!!!!")
		go svr.handleConnection(svr.count, c)
	}
}

func (svr *RpcServer) Stop() {
	if svr.l != nil {
		svr.l.Close()
	}
	svr.lock.RLock()
	for _, c := range svr.conns {
		c.Close()
	}
	svr.lock.RUnlock()
	svr.conns = make(map[uint64]net.Conn)
	svr.wg.Wait()
}

func (svr *RpcServer) handleConnection(id uint64, c net.Conn) {
	defer func () {
		fmt.Println("================connect closed")
		c.Close()
		svr.lock.Lock()
		delete(svr.conns, id)
		svr.lock.Unlock()
		svr.wg.Done()
	}()
	message := &Message{}
	for {
        err := util.ReadMessage(c, message)
		if err != nil {
			return
		}
		fmt.Println("===========read message ", message)
		switch message.GetFuncId() {
		case uint16(funcpb.FunctionID_kFuncHeartbeat):

		case uint16(funcpb.FunctionID_kFuncRawGet):
			resp := kvrpcpb.DsKvRawGetResponse{
				Header: &kvrpcpb.ResponseHeader{},
				Resp: &kvrpcpb.KvRawGetResponse{
					Code: 0,
					Value: []byte("ok"),
				},
			}
			data, _ := resp.Marshal()
			message.SetData(data)
		}
		message.SetMsgType(uint16(0x12))
		err = util.WriteMessage(c, message)
		if err != nil {
			fmt.Println("=============write failed ", err)
			return
		}
	}
}