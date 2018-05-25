package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"master-server/raft/proto"
)

type commandHandler func(s *server, argv []string) (string, error)

type commandInfo struct {
	argc    int
	handler commandHandler
	usage   string
}

var commands = map[string]*commandInfo{
	"submit": &commandInfo{
		argc:    2,
		handler: submitCommand,
		usage:   "submit new request. usage: submit {number}",
	},
	"member": &commandInfo{
		argc:    3,
		handler: memberCommand,
		usage:   "change raft memeber. usage: member {add|remove} {nodeID}",
	},
	"info": &commandInfo{
		argc:    1,
		handler: infoCommand,
		usage:   "prinf info. usage: \r\n\tinfo\r\n\tinfo leader\r\n\tinfo term\r\n\tinfo member\r\n\tinfo sum\r\n\tinfo replica",
	},
	"elect": &commandInfo{
		argc:    1,
		handler: electCommand,
		usage:   "propose current node try to leader",
	},
	"status": &commandInfo{
		argc:    1,
		handler: statusCommand,
		usage:   "print raft status",
	},
}

func submitCommand(s *server, argv []string) (string, error) {
	num, err := strconv.Atoi(argv[1])
	if err != nil {
		return "", err
	}
	cmd := make([]byte, 8)
	binary.BigEndian.PutUint64(cmd, uint64(num))
	f := s.rs.Submit(context.Background(), *groupID, cmd)
	resp, err := f.Response()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Result: %v", resp), nil
}

func memberCommand(s *server, argv []string) (string, error) {
	var ctype proto.ConfChangeType
	switch argv[1] {
	case "add":
		ctype = proto.ConfAddNode
	case "remove":
		ctype = proto.ConfRemoveNode
	default:
		return "", errors.New("unknow member change type")
	}

	nodeID, err := strconv.Atoi(argv[2])
	if err != nil {
		return "", err
	}
	peer := proto.Peer{ID: uint64(nodeID)}
	f := s.rs.ChangeMember(context.Background(), *groupID, ctype, peer, nil)
	resp, err := f.Response()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Result: %v", resp), nil
}

func infoCommand(s *server, argv []string) (string, error) {
	if len(argv) >= 2 {
		switch argv[1] {
		case "leader":
			l, t := s.rs.LeaderTerm(*groupID)
			_ = t
			return fmt.Sprintf("Leader: %d", l), nil
		case "term":
			l, t := s.rs.LeaderTerm(*groupID)
			_ = l
			return fmt.Sprintf("Term: %d", t), nil
		case "member":
			all := s.r.AllNodes()
			str := "["
			for i, n := range all {
				if i == len(all)-1 {
					str += fmt.Sprintf("%d]", n)
				} else {
					str += fmt.Sprintf("%d,", n)
				}
			}
			return str, nil
		case "sum":
			return fmt.Sprintf("%d", s.sm.current()), nil
		case "replica":
			if !s.rs.IsLeader(*groupID) {
				return "", errors.New("Not Leader")
			}
			downs := s.rs.GetDownReplicas(*groupID)
			pendings := s.rs.GetPendingReplica(*groupID)
			return fmt.Sprintf("down replica: %v\r\npending replica: %v", downs, pendings), nil
		}
	}
	return s.rs.Status(*groupID).String(), nil
}

func electCommand(s *server, argv []string) (string, error) {
	f := s.rs.TryToLeader(context.Background(), *groupID)
	resp, err := f.Response()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Result: %v", resp), nil
}

func statusCommand(s *server, argv []string) (string, error) {
	status := s.rs.Status(*groupID)
	return status.String(), nil
}

func helpMessage() (s string) {
	s += "support commands:\r\n"
	for c, info := range commands {
		s += c + "\r\n"
		s += "\t" + info.usage + "\r\n"
	}
	return
}
