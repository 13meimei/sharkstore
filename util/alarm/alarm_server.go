package alarm

import (
	"context"
	"net"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"model/pkg/alarmpb"
	"fmt"
	"errors"
	"util/log"
	"time"
)

type AlarmServer interface {
	TaskAlarm(context.Context, *alarmpb.TaskAlarmRequest) (*alarmpb.TaskAlarmResponse, error)
	NodeRangeAlarm(context.Context, *alarmpb.NodeRangeAlarmRequest) (*alarmpb.NodeRangeAlarmResponse, error)
	AliveAlarm(context.Context, *alarmpb.AliveRequest) (*alarmpb.AliveResponse, error)
}

type Server struct {
	gateway *MessageGateway

}

func newServer(ctx context.Context, gatewayAddr string, receiver MessageReceiver) *Server {
	gateway := NewMessageGateway(ctx, gatewayAddr, receiver)
	if gateway == nil {
		return nil
	}

	return &Server{
		gateway: gateway,
	}
}


func NewAlarmServer(ctx context.Context, port int, gatewayAddress string, receiver MessageReceiver) (*Server, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil, err
	}
	s := grpc.NewServer()
	server := newServer(ctx, gatewayAddress, receiver)
	if server == nil {
		return nil, errors.New("server is nil")
	}
	// register alarm server
	alarmpb.RegisterAlarmServer(s, server)
	reflection.Register(s)
	go s.Serve(lis)

	return server, nil
}

func (s *Server) TaskAlarm(ctx context.Context, req *alarmpb.TaskAlarmRequest) (*alarmpb.TaskAlarmResponse, error) {
	var err error
	resp := new(alarmpb.TaskAlarmResponse)
	clusterId := req.GetHead().GetClusterId()
	// todo
	switch req.GetType() {
	case alarmpb.TaskAlarmType_TIMEOUT:
	case alarmpb.TaskAlarmType_LONG_TIME_RUNNING:
	}
	if err := s.gateway.notify(Message{
		ClusterId: clusterId,
		Title: "task timeout",
		Content: req.GetDescribe(),
	}, time.Second); err != nil {
		log.Error("task alarm notify timeout")
	}

	return resp, err
}

func (s *Server) NodeRangeAlarm(ctx context.Context, req *alarmpb.NodeRangeAlarmRequest) (*alarmpb.NodeRangeAlarmResponse, error) {
	var err error
	resp := new(alarmpb.NodeRangeAlarmResponse)
	clusterId := req.GetHead().GetClusterId()
	// todo
	switch req.GetType() {
	case alarmpb.NodeRangeAlarmType_RANGE_NO_HEARTBEAT:
	case alarmpb.NodeRangeAlarmType_NODE_NO_HEARTBEAT:
	case alarmpb.NodeRangeAlarmType_NODE_DISK_SIZE:
	case alarmpb.NodeRangeAlarmType_NODE_LEADER_COUNT:
	}
	if err := s.gateway.notify(Message{
		ClusterId: clusterId,
		Title: "node/range alarm",
		Content: req.GetDescribe(),
	}, time.Second); err != nil {
		log.Error("node/range alarm notify timeout")
	}

	return resp, err
}

func (s *Server) AliveAlarm(ctx context.Context, req *alarmpb.AliveRequest) (*alarmpb.AliveResponse, error) {
	var err error
	resp := new(alarmpb.AliveResponse)
	// todo
	return resp, err
}


