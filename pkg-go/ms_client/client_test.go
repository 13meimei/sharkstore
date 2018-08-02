package client

import (
	"testing"
	"context"
	"time"

	"model/pkg/mspb"

	"google.golang.org/grpc"
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