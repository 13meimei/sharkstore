package client

import (
	"google.golang.org/grpc"
	"model/pkg/mspb"
	"util/assert"
	"testing"
	"context"
	"time"
	"util/log"
	"sync"
)

//func TestNewClient(t *testing.T) {
//	client,err := NewClient([]string{"192.168.61.136:8887"})
//	if err != nil {
//		fmt.Errorf("%v",err)
//	}
//
//	resp,err := client.AddNode(&mspb.AddNodeRequest{
//		Header:&mspb.RequestHeader{
//			ClusterId:1,
//		},
//		ServerPort:8887,
//		HeartbeatPort:8877,
//		ReplicaPort:8867,
//	})
//
//	fmt.Println(resp)
//}

func TestRPCClient_GetRoute(t *testing.T) {
	conn, err := grpc.Dial("127.0.0.1:18887", grpc.WithInsecure(), grpc.WithTimeout(ConnectMSTimeout))
	if err != nil {
		t.Errorf("did not connect addr[%s], err[%v]", "127.0.0.1:18887", err)
		return
	}
	cli := mspb.NewMsServerClient(conn)
	req := &mspb.GetRouteRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    1,
		TableId: 1,
		Key:     []byte{1},
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	response, err := cli.GetRoute(ctx, req)
	if err != nil {
		t.Logf("error : %v", err)
		return
	}
	t.Logf("response: %v", response)
}

func TestRPCClient_GetAutoIncId(t *testing.T) {
	conn, err := grpc.Dial("192.168.150.122:18887", grpc.WithInsecure(), grpc.WithTimeout(ConnectMSTimeout))
	if err != nil {
		t.Errorf("did not connect addr[%s], err[%v]", "192.168.150.122:18887", err)
		return
	}
	cli := mspb.NewMsServerClient(conn)
	req := &mspb.GetAutoIncIdRequest{
		Header:  &mspb.RequestHeader{},
		DbId:    161,
		TableId: 169,
		Size_:   uint32(40),
	}
	for loop:=0;;loop++{
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		response, err := cli.GetAutoIncId(ctx, req)
		if err != nil {
			t.Logf("loop: %v error : %v", loop, err)
			return
		}
		//log.Info("%v", response)
		assert.Equal(t, uint32(len(response.GetIds())), req.Size_, "return size error")
	}

}

var wg sync.WaitGroup  //定义一个同步等待的组
func TestRPCClient_GetMsLeader(t *testing.T) {
	conn, err := grpc.Dial("192.168.183.66:18887", grpc.WithInsecure(), grpc.WithTimeout(ConnectMSTimeout))
	if err != nil {
		t.Errorf("did not connect addr[%s], err[%v]", "192.168.183.66:18887", err)
		return
	}
	cli := mspb.NewMsServerClient(conn)
	req := &mspb.GetMSLeaderRequest{
		Header:  &mspb.RequestHeader{},
	}
	for loop:=0;;loop++{
		if loop %300 == 0 {
			time.Sleep(time.Duration(time.Second))
		}
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		wg.Add(1)
		go func(loop int) {
			defer wg.Done()
			response, err := cli.GetMSLeader(ctx, req)
			if err != nil {
				t.Logf("error : %v", err)
				return
			}
			if response.GetLeader().GetId() == 0 {
				t.Logf("error : %v", err)
				return
			}
			log.Info("loop %v info %v", loop, response)

		}(loop)
	}
	wg.Wait()
}