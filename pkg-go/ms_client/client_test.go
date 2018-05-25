package client

import (
	"testing"
	"fmt"
	"model/pkg/mspb"
)

func TestNewClient(t *testing.T) {
	client,err := NewClient([]string{"192.168.61.136:8887"})
	if err != nil {
		fmt.Errorf("%v",err)
	}

	resp,err := client.AddNode(&mspb.AddNodeRequest{
		Header:&mspb.RequestHeader{
			ClusterId:1,
		},
		ServerPort:8887,
		HeartbeatPort:8877,
		ReplicaPort:8867,
	})

	fmt.Println(resp)
}