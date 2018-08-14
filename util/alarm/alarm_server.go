package alarm

import (
	"fmt"
	"sync"
	"strconv"
	"errors"
	"net"
	"net/http"
	"time"
	"util/log"
	"util/deepcopy"
	"model/pkg/alarmpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"golang.org/x/net/context"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

const (
	APP_NAME_GATEWAY = "gw"
)

type AlarmServer interface {
	NodeRangeAlarm(context.Context, *alarmpb.NodeRangeAlarmRequest) (*alarmpb.NodeRangeAlarmResponse, error)
	SimpleAlarm(ctx context.Context, req *alarmpb.SimpleRequest) (*alarmpb.SimpleResponse, error)
}

type Server struct {
	gateway *MessageGateway
	filters []alarmFilter

	aliveCheckingAppKeys 	[]string
	aliveCheckingLock 	sync.RWMutex

	//aliveAppsRemote *JimdbAPClient // todo jimdb ap client
}

func newServer(ctx context.Context, alarmServerAddr string) *Server {
	gateway := NewMessageGateway(ctx, alarmServerAddr)
	if gateway == nil {
		return nil
	}

	s := new(Server)
	s.gateway = gateway
	s.filters = append(s.filters, new(clusterIdFilter))

	return s
}

func (s *Server) genAliveAppKey(appName string, clusterId uint64, appAddr string) string {
	return fmt.Sprintf("alive_%v_%v_%v", appName, clusterId, appAddr)
}

func (s *Server) AddAliveCheckingAppAddr(appName string, clusterId uint64, appAddr string) {
	appKey := s.genAliveAppKey(appName, clusterId, appAddr)

	s.aliveCheckingLock.Lock()
	defer s.aliveCheckingLock.Unlock()

	s.aliveCheckingAppKeys = append(s.aliveCheckingAppKeys, appKey)
}

func (s *Server) CopyAliveCheckingAppKeys() (ret []string) {
	s.aliveCheckingLock.Lock()
	defer s.aliveCheckingLock.Unlock()

	if (len(s.aliveCheckingAppKeys) == 0) {
		return
	}

	ret = deepcopy.Iface(s.aliveCheckingAppKeys).([]string)
	s.aliveCheckingAppKeys = nil
	return
}

func NewAlarmServer(ctx context.Context, port int, remoteAlarmServerAddr string) (*Server, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil, err
	}
	s := grpc.NewServer()
	server := newServer(ctx, remoteAlarmServerAddr)
	if server == nil {
		return nil, errors.New("server is nil")
	}
	// register alarm server
	alarmpb.RegisterAlarmServer(s, server)
	reflection.Register(s)
	go s.Serve(lis)

	return server, nil
}

func (s *Server) parseHost(host string) ([]string, error) {
	gwHostInfo := strings.SplitN(host, ":", 2)
	if len(gwHostInfo) != 2 {
		return nil, errors.New("splitN failed != input arg N")
	}
	gwHost := gwHostInfo[0]
	_, err := strconv.ParseInt(gwHostInfo[1], 10, 64)
	//gwPort, err := strconv.ParseInt(gwHostInfo[1], 10, 64)
	if err != nil {
		return nil, err
	}

	addrs, err := net.LookupHost(gwHost)
	if err != nil {
		return nil, err
	}

	return addrs, nil
}

func (s *Server) loadAliveCheckingAppAddrs() error {
	clusterInfos, err := s.getClusterInfo()
	if err != nil {
		return err
	}
	for _, info := range clusterInfos {
		if len(info.remark) != 0 {
			continue // fixme domain host is vip
		}

		// do with gw
		gwAddrs, err := s.parseHost(info.appGwHost)
		if err != nil {
			log.Error("parse gateway domain failed: %v", err)
			continue
		}

		for _, gwAddr := range gwAddrs {
			s.AddAliveCheckingAppAddr(APP_NAME_GATEWAY, info.id, gwAddr)
		}

	}

	return nil
}

type ClusterInfo struct {
	id uint64
	appName string
	appGwHost string
	appMsHost string
	remark string
}

func (s *Server) getClusterInfo() (clusterInfos []ClusterInfo, err error) {
	db, err := sql.Open("mysql", "test:123456@tcp(sharkstore.gw1.jd.com:3360)/fbase")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var (
		clusterId uint64
		gwHost string
		remark string
	)
	rows, err := db.Query("select id, gateway_sql, remark from fbase_cluster")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&clusterId, &gwHost, &remark)
		if err != nil {
			return nil, err
		}
		clusterInfos = append(clusterInfos, ClusterInfo{
			id: clusterId,
			appName: APP_NAME_GATEWAY,
			appGwHost: gwHost,
			remark: remark,
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return
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

	for _, f := range s.filters {
		if ok := f.FilteredByInt(req.GetHead().GetClusterId()); ok {
			return resp, nil
		}
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

func HandleAppPing(w http.ResponseWriter, r *http.Request) {
	log.Debug("handle app ping")

	//app := r.FormValue("app")
	//ips := r.FormValue("ips")
	//ttl, _ := strconv.ParseUint(r.FormValue("ping_interval"), 10, 64)
	//
	//s.genAliveAppKey()

	// fixme setex key with ttl ping_interval to jimdb

}

func (s *Server) AliveCheckingAlarm() {
	log.Debug("alive checking alarm processing...")

	t := time.NewTicker(10 *time.Second)
	for {
		select {
		case <-t.C:
			if err := s.loadAliveCheckingAppAddrs(); err != nil {
				log.Error("loadAliveCheckingAppAddrs failed: %v", err)
				continue
			}

			appKeys := s.CopyAliveCheckingAppKeys()
			for _, key := range appKeys {
				// fixme get app in jimdb, if it is not in
				key = key

				if err := s.gateway.notify(Message{
					//ClusterId: clusterId,
					//Title: req.GetTitle(),
					//Content: req.GetContent(),
					//samples: req.SampleJson,
				}, time.Second); err != nil {
					log.Error("alive alarm notify timeout")
				}
			}

		}
	}
}

func (s *Server) SimpleAlarm(ctx context.Context, req *alarmpb.SimpleRequest) (*alarmpb.SimpleResponse, error) {
	log.Info("receive simple alarm message: %v", req.String())
	var err error
	resp := new(alarmpb.SimpleResponse)

	clusterId := req.GetHead().GetClusterId()

	for _, f := range s.filters {
		if ok := f.FilteredByInt(req.GetHead().GetClusterId()); ok {
			return resp, nil
		}
	}
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
