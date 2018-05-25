package client

import (
	"model/pkg/mspb"
	"google.golang.org/grpc"
)

type Conn struct {
	addr   string
	conn   *grpc.ClientConn
	Cli    mspb.MsServerClient

	closed bool
}

func (c *Conn) Close() {
	if c.closed {
		return
	}
	c.closed = true
	c.conn.Close()
}
