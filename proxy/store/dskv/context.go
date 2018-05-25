package dskv

import (
	"model/pkg/kvrpcpb"
	"time"
)

type Context struct {
	VID     RangeVerID
	RequestHeader *kvrpcpb.RequestHeader
	NodeId  uint64
	NodeAddr string
	Timeout time.Duration
}
