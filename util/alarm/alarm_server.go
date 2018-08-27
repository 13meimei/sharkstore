package alarm

import (
	"fmt"
	"sync"
	"strings"
	"strconv"
	"errors"
	"net"
	"net/http"
	"time"
	"util/log"
	"model/pkg/alarmpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"golang.org/x/net/context"

	"github.com/gomodule/redigo/redis"
)


type AlarmServer interface {
	NodeRangeAlarm(context.Context, *alarmpb.NodeRangeAlarmRequest) (*alarmpb.NodeRangeAlarmResponse, error)
	SimpleAlarm(ctx context.Context, req *alarmpb.SimpleRequest) (*alarmpb.SimpleResponse, error)
}

type Server struct {
	gateway *MessageGateway

	aliveCheckingAppKeys []aliveAppKey
	aliveCheckingLock sync.RWMutex

	sqlArgs string

	jimUrl string
	jimApAddr string
	jimConnTimeoutSec time.Duration
	jimWriteTimeoutSec time.Duration
	jimReadTimeoutSec time.Duration
	jimClientPool *redis.Pool
}

func newServer(ctx context.Context, alarmServerAddr string, sqlArgs, jimUrl, jimApAddr string) *Server {
	gateway := NewMessageGateway(ctx, alarmServerAddr)
	if gateway == nil {
		return nil
	}

	s := new(Server)
	s.gateway = gateway

	s.sqlArgs = sqlArgs
	s.jimUrl = jimUrl
	s.jimApAddr = jimApAddr
	s.jimClientPool = &redis.Pool{
		MaxIdle: 3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return s.jimDial()
		},
	}

	go s.aliveCheckingAlarm()
	return s
}

func NewAlarmServer(ctx context.Context, port int, remoteAlarmServerAddr string, sqlArgs, jimUrl, jimApAddr string) (*Server, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil, err
	}
	s := grpc.NewServer()
	server := newServer(ctx, remoteAlarmServerAddr, sqlArgs, jimUrl, jimApAddr)
	if server == nil {
		return nil, errors.New("server is nil")
	}
	// register alarm server
	alarmpb.RegisterAlarmServer(s, server)
	reflection.Register(s)
	go s.Serve(lis)

	return server, nil
}

func (s *Server) NodeRangeAlarm(ctx context.Context, req *alarmpb.NodeRangeAlarmRequest) (*alarmpb.NodeRangeAlarmResponse, error) {
	log.Info("receive node/range alarm message: %v", req.String())
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
		samples: req.SampleJson,
	}, time.Second); err != nil {
		log.Error("node/range alarm notify timeout")
	}

	return resp, err
}

func (s *Server) HandleAppPing(w http.ResponseWriter, r *http.Request) {
	log.Debug("handle app ping")

	resp := "ok"
	defer w.Write([]byte(resp))

	appName := r.FormValue("app_name") // app argv[0]
	appClusterId := r.FormValue("cluster_id")
	ipAddrs := r.FormValue("ip_addrs") // string ip join with ',': ip1,ip2,...
	port, err := strconv.ParseUint(r.FormValue("port"), 10, 64)
	if err != nil {
		resp = fmt.Sprintf("port parseuint failed: %v", err)
		log.Error(resp)
		return
	}
	ttl, err := strconv.ParseUint(r.FormValue("ping_interval"), 10, 64)
	if err != nil {
		resp = fmt.Sprintf("ping_interval parseuint failed: %v", err)
		log.Error(resp)
		return
	}
	ipsArr := strings.Split(ipAddrs, ",")
	if len(ipsArr) == 0 {
		resp = fmt.Sprintf("ip_addrs can not be split with letter ','")
		log.Error(resp)
		return
	}

	for _, ip := range ipsArr {
		appAddr := fmt.Sprintf("%v:%v", ip, port)
		appKey := newAliveAppKey(appName, appClusterId, appAddr)
		log.Info("gen app key: %v", appKey)
		if len(appKey) == 0 {
			resp = fmt.Sprintf("app name [%v] can not be generated", appName)
			log.Error(resp)
			return
		}

		// setex key with ttl ping_interval to jimdb
		reply, err := s.jimSendCommand("setex",  appKey, ttl*2, "")
		replyStr, err := redis.String(reply, err)
		if err != nil {
			resp = fmt.Sprintf("jim command setex reply type is not int: %v", err)
			log.Error(resp)
			return
		}
		if (strings.Compare(replyStr, "ok") != 0) {
			resp = fmt.Sprintf("jim command setex reply is not ok")
			log.Error(resp)
			return
		}
	}
}

func (s *Server) SimpleAlarm(ctx context.Context, req *alarmpb.SimpleRequest) (*alarmpb.SimpleResponse, error) {
	log.Info("receive simple alarm message: %v", req.String())
	var err error
	resp := new(alarmpb.SimpleResponse)

	clusterId := req.GetHead().GetClusterId()

	if err := s.gateway.notify(Message{
		ClusterId: clusterId,
		Title: req.GetTitle(),
		Content: req.GetContent(),
		samples: req.SampleJson,
	}, time.Second); err != nil {
		log.Error("simple alarm notify timeout")
	}

	return resp, err
}
