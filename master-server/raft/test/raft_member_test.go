package test

import (
	"fmt"
	"testing"
	"time"

	"master-server/raft/proto"
)

func TestMemberWithNoLease(t *testing.T) {
	f, w := getLogFile("changemember_nolease.log")
	defer func() {
		w.Flush()
		f.Close()
	}()

	servers := initTestServer(peers, false, true)
	fmt.Println("waiting electing leader....")
	leadServer := waitElect(servers, w)
	printStatus(servers, w)

	// test add node
	w.WriteString(fmt.Sprintf("[%s] Add new node \r\n", time.Now().Format(format_time)))
	leader, term := leadServer.raft.LeaderTerm(1)
	newServer := createRaftServer(4, leader, term, peers, false, true)
	// add node
	resolver.addNode(4, 0)
	fmt.Println("starting add node")
	leadServer.sm.AddNode(proto.Peer{ID: 4})
	fmt.Println("added node")
	time.Sleep(time.Second)
	servers = append(servers, newServer)
	printStatus(servers, w)

	fmt.Println("starting put data")
	if err := leadServer.sm.Put("test2", "test2_val"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if vget, err := newServer.sm.Get("test2"); err != nil || vget != "test2_val" {
		t.Fatal("new add node not get the data")
	}
	fmt.Println("success put data")

	// test remove node
	w.WriteString(fmt.Sprintf("[%s] Remove node \r\n", time.Now().Format(format_time)))
	fmt.Println("starting remove node")
	leadServer.sm.RemoveNode(proto.Peer{ID: 4})
	fmt.Println("removed node")
	fmt.Println("starting put data")
	if err := leadServer.sm.Put("test3", "test3_val"); err != nil {
		t.Fatal(err)
	}
	fmt.Println("success put data")
	newServers := make([]*testServer, 0)
	for _, s := range servers {
		if s.nodeID == newServer.nodeID {
			s.raft.Stop()
		} else {
			newServers = append(newServers, s)
		}
	}
	servers = newServers
	time.Sleep(100 * time.Millisecond)
	newServer = createRaftServer(4, 0, 10, append(peers, proto.Peer{ID: 4}), false, false)
	servers = append(servers, newServer)
	time.Sleep(10 * time.Second)
	printStatus(servers, w)
	resolver.delNode(4)

	for _, s := range servers {
		s.raft.Stop()
	}
	time.Sleep(100 * time.Millisecond)
}

func TestMemberWithLease(t *testing.T) {
	f, w := getLogFile("changemember_lease.log")
	defer func() {
		w.Flush()
		f.Close()
	}()

	servers := initTestServer(peers, true, true)
	fmt.Println("waiting electing leader....")
	leadServer := waitElect(servers, w)
	printStatus(servers, w)

	// test add node
	w.WriteString(fmt.Sprintf("[%s] Add new node \r\n", time.Now().Format(format_time)))
	leader, term := leadServer.raft.LeaderTerm(1)
	newServer := createRaftServer(4, leader, term, peers, true, true)
	// add node
	resolver.addNode(4, 0)
	fmt.Println("starting add node")
	leadServer.sm.AddNode(proto.Peer{ID: 4})
	fmt.Println("added node")
	time.Sleep(time.Second)
	servers = append(servers, newServer)
	printStatus(servers, w)

	fmt.Println("starting put data")
	if err := leadServer.sm.Put("test2", "test2_val"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if vget, err := newServer.sm.Get("test2"); err != nil || vget != "test2_val" {
		t.Fatal("new add node not get the data")
	}
	fmt.Println("success put data")

	// test remove node
	w.WriteString(fmt.Sprintf("[%s] Remove node \r\n", time.Now().Format(format_time)))
	fmt.Println("starting remove node")
	leadServer.sm.RemoveNode(proto.Peer{ID: 4})
	fmt.Println("removed node")
	fmt.Println("starting put data")
	if err := leadServer.sm.Put("test3", "test3_val"); err != nil {
		t.Fatal(err)
	}
	fmt.Println("success put data")
	newServers := make([]*testServer, 0)
	for _, s := range servers {
		if s.nodeID == newServer.nodeID {
			s.raft.Stop()
		} else {
			newServers = append(newServers, s)
		}
	}
	servers = newServers
	time.Sleep(100 * time.Millisecond)
	newServer = createRaftServer(4, 0, 10, append(peers, proto.Peer{ID: 4}), false, false)
	servers = append(servers, newServer)
	time.Sleep(10 * time.Second)
	printStatus(servers, w)
	resolver.delNode(4)

	for _, s := range servers {
		s.raft.Stop()
	}

	time.Sleep(100 * time.Millisecond)
}
