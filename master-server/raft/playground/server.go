package main

import (
	"bufio"
	"flag"
	"net"
	"path"
	"strconv"
	"strings"
	"time"

	"fmt"

	"master-server/raft"
	"master-server/raft/proto"
	"master-server/raft/storage/wal"
	"util/log"
)

var node = flag.Int("node", 0, "node id")
var dir = flag.String("dir", "./data", "data dir")
var peers = flag.String("peers", "", "peers, example: 1,2,3")
var hole = flag.Bool("hole", false, "init raft log with hole")
var groupID = flag.Uint64("gid", 1, "raft group id")

func main() {
	flag.Parse()

	s := startServer()
	s.runService()
}

type server struct {
	nodeID uint64

	addr *address
	r    *resolver
	rs   *raft.RaftServer
	sm   *stateMachine
	l    net.Listener
}

func startServer() *server {
	s := &server{}

	s.nodeID = uint64(*node)
	if s.nodeID == 0 || s.nodeID > 9 {
		log.Panic("invalid nodeID, should be in [1-9]")
	}
	log.Info("start node %d.", s.nodeID)

	// address
	addrInfo, ok := addrDatabase[s.nodeID]
	if !ok {
		log.Panic("no such address info. nodeId: %d", s.nodeID)
	}
	s.addr = addrInfo

	// resolver
	r := newResolver()
	s.r = r

	//  new raft server
	c := raft.DefaultConfig()
	c.TickInterval = time.Millisecond * 200
	c.NodeID = s.nodeID
	c.Resolver = r
	c.HeartbeatAddr = addrInfo.heartbeat
	c.ReplicateAddr = addrInfo.replicate
	rs, err := raft.NewRaftServer(c)
	if err != nil {
		log.Panic("create raft server failed: %v", err)
	}
	s.rs = rs

	// raft log stroage
	var wc *wal.Config
	if *hole {
		wc = &wal.Config{
			TruncateFirstDummy: true,
		}
	}
	raftStroage, err := wal.NewStorage(*groupID, path.Join(*dir, "wal"), wc)
	if err != nil {
		log.Panic("new raft log stroage error: %v", err)
	}
	// parse peers
	raftPeers, err := parsePeers(*peers)
	if err != nil {
		log.Panic("parse peers failed!. peers=%v", *peers)
	}
	for _, p := range raftPeers {
		r.addNode(p.ID)
	}
	// state machine
	s.sm = newStateMachine(s.nodeID, r)
	rc := &raft.RaftConfig{
		ID:           *groupID,
		Peers:        raftPeers,
		Storage:      raftStroage,
		StateMachine: s.sm,
	}
	err = rs.CreateRaft(rc)
	if err != nil {
		log.Panic("creat raft failed. %v", err)
	}

	return s
}

func (s *server) runService() {
	l, err := net.Listen("tcp", s.addr.client)
	if err != nil {
		log.Panic("listen on %v failed: %v", s.addr.client, err)
	}

	log.Info("start service client on %v", s.addr.client)

	s.l = l

	for {
		conn, err := s.l.Accept()
		if err != nil {
			log.Panic("accept error: %v", err)
		}
		go s.handleConn(conn)
	}
}

func (s *server) handleConn(conn net.Conn) {
	defer func() {
		if x := recover(); x != nil {
			log.Error("connection occur error: %v", x)
		}
	}()

	defer func() {
		conn.Close()
	}()

	response := func(s string) {
		_, err := conn.Write([]byte("> " + s + "\r\n"))
		if err != nil {
			panic(err)
		}
	}

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		argv := strings.Split(scanner.Text(), " ")
		if len(argv) == 0 {
			response("ERR: invalid command")
			continue
		}

		switch argv[0] {
		case "q":
			return
		case "quit":
			return
		case "help":
			response(helpMessage())
			continue
		}

		cmdinfo, ok := commands[argv[0]]
		if !ok {
			response("ERR: unsupported command")
			continue
		}
		if len(argv) < cmdinfo.argc {
			response("ERR: wrong command paramters")
			continue
		}
		ret, err := cmdinfo.handler(s, argv)
		if err != nil {
			response(fmt.Sprintf("%v", ret))
		} else {
			response(ret)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Error("read connection error: %v", err)
	}
}

func parsePeers(str string) (peers []proto.Peer, err error) {
	strPeers := strings.Split(str, ",")
	for _, s := range strPeers {
		p, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		peers = append(peers, proto.Peer{ID: uint64(p)})
	}
	return
}
