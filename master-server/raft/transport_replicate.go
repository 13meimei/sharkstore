package raft

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"master-server/raft/logger"
	"master-server/raft/proto"
	"master-server/raft/util"
)

type replicateTransport struct {
	config      *TransportConfig
	raftServer  *RaftServer
	listener    net.Listener
	curSnapshot int32
	mu          sync.RWMutex
	senders     map[uint64]*transportSender
	stopc       chan struct{}
}

func newReplicateTransport(raftServer *RaftServer, config *TransportConfig) (*replicateTransport, error) {
	var (
		listener net.Listener
		err      error
	)

	if listener, err = net.Listen("tcp", config.ReplicateAddr); err != nil {
		return nil, err
	}
	t := &replicateTransport{
		config:     config,
		raftServer: raftServer,
		listener:   listener,
		senders:    make(map[uint64]*transportSender),
		stopc:      make(chan struct{}),
	}
	return t, nil
}

func (t *replicateTransport) stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	select {
	case <-t.stopc:
		return
	default:
		close(t.stopc)
		t.listener.Close()
		for _, s := range t.senders {
			s.stop()
		}
	}
}

func (t *replicateTransport) send(m *proto.Message) {
	s := t.getSender(m.To)
	s.send(m)
}

func (t *replicateTransport) getSender(nodeId uint64) *transportSender {
	t.mu.RLock()
	sender, ok := t.senders[nodeId]
	t.mu.RUnlock()
	if ok {
		return sender
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if sender, ok = t.senders[nodeId]; !ok {
		sender = newTransportSender(nodeId, uint64(t.config.MaxReplConcurrency), t.config.SendBufferSize, Replicate, t.config.Resolver)
		t.senders[nodeId] = sender
	}
	return sender
}

func (t *replicateTransport) sendSnapshot(m *proto.Message, rs *snapshotStatus) {
	var (
		conn *util.ConnTimeout
		err  error
	)
	defer func() {
		atomic.AddInt32(&t.curSnapshot, -1)
		rs.respond(err)
		if conn != nil {
			conn.Close()
		}
		if err != nil {
			logger.Error("[Transport] %v send snapshot to %v failed error is: %v.", m.ID, m.To, err)
		} else if logger.IsEnableWarn() {
			logger.Warn("[Transport] %v send snapshot to %v successful.", m.ID, m.To)
		}
	}()

	if atomic.AddInt32(&t.curSnapshot, 1) > int32(t.config.MaxSnapConcurrency) {
		err = fmt.Errorf("snapshot concurrency exceed the limit %v.", t.config.MaxSnapConcurrency)
		return
	}
	if conn = getConn(m.To, Replicate, t.config.Resolver, 10*time.Minute, 15*time.Second); conn == nil {
		err = fmt.Errorf("can't get connection to %v.", m.To)
		return
	}

	// send snapshot header message
	bufWr := util.NewBufferWriter(conn, 1*MB)
	if err = m.Encode(bufWr); err != nil {
		return
	}
	if err = bufWr.Flush(); err != nil {
		return
	}

	// send snapshot data
	var (
		data      []byte
		loopCount = 0
		sizeBuf   = make([]byte, 4)
	)
	for err == nil {
		loopCount = loopCount + 1
		if loopCount > 16 {
			loopCount = 0
			runtime.Gosched()
		}

		select {
		case <-rs.stopCh:
			err = fmt.Errorf("raft has shutdown.")

		default:
			data, err = m.Snapshot.Next()
			if len(data) > 0 {
				// write block size
				binary.BigEndian.PutUint32(sizeBuf, uint32(len(data)))
				if _, err = bufWr.Write(sizeBuf); err == nil {
					_, err = bufWr.Write(data)
				}
			}
		}
	}

	// write end flag and flush
	if err != nil && err != io.EOF {
		return
	}
	binary.BigEndian.PutUint32(sizeBuf, 0)
	if _, err = bufWr.Write(sizeBuf); err != nil {
		return
	}
	if err = bufWr.Flush(); err != nil {
		return
	}

	// wait response
	err = nil
	resp := make([]byte, 1)
	io.ReadFull(conn, resp)
	if resp[0] != 1 {
		err = fmt.Errorf("follower response failed.")
	}
}

func (t *replicateTransport) start() {
	util.RunWorkerUtilStop(func() {
		for {
			select {
			case <-t.stopc:
				return
			default:
				conn, err := t.listener.Accept()
				if err != nil {
					continue
				}
				t.handleConn(util.NewConnTimeout(conn))
			}
		}
	}, t.stopc)
}

func (t *replicateTransport) handleConn(conn *util.ConnTimeout) {
	util.RunWorker(func() {
		defer conn.Close()

		loopCount := 0
		bufRd := util.NewBufferReader(conn, 16*KB)
		for {
			loopCount = loopCount + 1
			if loopCount > 16 {
				loopCount = 0
				runtime.Gosched()
			}

			select {
			case <-t.stopc:
				return
			default:
				if msg, err := reciveMessage(bufRd); err != nil {
					return
				} else {
					if msg.Type == proto.ReqMsgSnapShot {
						if err := t.handleSnapshot(msg, conn, bufRd); err != nil {
							return
						}
					} else {
						t.raftServer.reciveMessage(msg)
					}
				}
			}
		}
	})
}

var snap_ack = []byte{1}

func (t *replicateTransport) handleSnapshot(m *proto.Message, conn *util.ConnTimeout, bufRd *util.BufferReader) error {
	conn.SetReadTimeout(time.Minute)
	conn.SetWriteTimeout(15 * time.Second)
	bufRd.Grow(1 * MB)
	req := newSnapshotRequest(m, bufRd)
	t.raftServer.reciveSnapshot(req)

	// wait snapshot result
	if err := req.response(); err != nil {
		logger.Error("[Transport: %d] handle snapshot request from %v error: %v.", req.header.ID, m.From, err)
		return err
	}

	_, err := conn.Write(snap_ack)
	return err
}
