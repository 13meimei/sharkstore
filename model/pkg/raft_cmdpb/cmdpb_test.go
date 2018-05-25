package raft_cmdpb

import (
	"testing"
	"sync"
	"unsafe"
	"fmt"
	"model/pkg/kvrpcpb"
)


func TestRequest_Reset(t *testing.T) {
	pool :=&sync.Pool{
		New:func() interface{} {
			return &messagepb.Message{}
		},
	}

	msg := pool.Get().(*messagepb.Message)
	pool.Put
	msg.MsgType=messagepb.MessageType_KvReq
	msg.KvReq=&kvrpcpb.Request{}


	fmt.Printf("%v \n",unsafe.Sizeof(*msg))
	fmt.Printf("%v \n",unsafe.Sizeof(kvrpcpb.Request{}))
}
