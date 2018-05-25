package metric

import (
	"bufio"
	"strings"
	"net"
	"io"

	"util/log"
	"github.com/juju/errors"
	"encoding/binary"
)

const (
	readBufferSize  = 8 * 1024
	writeBufferSize = 8 * 1024
)

const (
	Type_Cluster = iota
	Type_Mac
	Type_Process
	Type_DB
	Type_Table
)

type Context struct {
	clusterId uint64
	namespace  string
	subsystem string
}

type conn struct {
	ctx *Context
	item int
	rb   *bufio.Reader
	wb   *bufio.Writer
	conn net.Conn
}

func (c *conn) close() error {
	if err := c.conn.Close(); isUnexpectedConnError(err) {
		return errors.Trace(err)
	}
	return nil
}

func (m *Metric) work(c *conn) {
	defer func() {
		m.wg.Done()
		c.close()

		m.connsLock.Lock()
		delete(m.conns, c)
		m.connsLock.Unlock()
	}()
	wr := bufio.NewReadWriter(c.rb, c.wb)
	for {
		data, err := readMessage(wr)
		if err != nil {
			log.Debug("read metric message failed, RemoteIp [%v],  err %v", c.conn.RemoteAddr().String(), err)
			return
		}
		switch c.item {
		case Type_Cluster:
			err = m.doClusterMetric(c.ctx, data)
		case Type_Mac:
			err = m.doMacMetric(c.ctx, data)
		case Type_Process:
			err = m.doProcessMetric(c.ctx, data)
		case Type_DB:
			err = m.doDbMetric(c.ctx, data)
		case Type_Table:
			err = m.doTableMetric(c.ctx, data)
		default:
			log.Warn("invalid metric item")
			return
		}
		if err != nil {
			log.Warn("do metric failed, err %v", err)
			return
		}
	}
}

func readMessage(r io.Reader) (data []byte, err error) {
	var header [4]byte
	var size int
	_, err = io.ReadFull(r, header[:])
	if err != nil {
		return
	}
	dataLen := binary.BigEndian.Uint32(header[0:4])
	data = make([]byte, dataLen)
	size, err = io.ReadFull(r, data)
	if err != nil {
		return
	}
	if size != int(dataLen) {
		err = errors.New("invalid message")
	}
	return
}

var errClosed = errors.New("use of closed network connection")

func isUnexpectedConnError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Cause(err) == io.EOF {
		return false
	}
	if strings.Contains(err.Error(), errClosed.Error()) {
		return false
	}
	return true
}