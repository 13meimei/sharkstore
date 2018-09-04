package alarm2

import (
	"fmt"
	"strings"

	"github.com/gomodule/redigo/redis"

	"util/log"
	"model/pkg/alarmpb2"
)

func (s *Server) handleAppHeartbeat(header *alarmpb2.RequestHeader, req *alarmpb2.AppHeartbeatRequest) (resp *alarmpb2.AlarmResponse, err error) {
	log.Debug("handle app heartbeat request: %v", req)
	resp = new(alarmpb2.AlarmResponse)

	aliveKey, err := newAliveKey(req.GetAppName(), header.GetClusterId(), header.GetIpAddr())
	if err != nil {
		resp.Header.Code = int64(alarmpb2.AlarmResponseCode_ALARM_ERROR)
		resp.Header.Error = err.Error()
		return
	}

	// setex key with ttl ping_interval to jimdb
	reply, err := s.jimCommand("setex", aliveKey, req.GetHbIntervalTime()*2, "")
	replyStr, err := redis.String(reply, err)
	if err != nil {
		resp.Header.Code = int64(alarmpb2.AlarmResponseCode_ALARM_ERROR)
		resp.Header.Error = err.Error()
		return
	}
	if strings.Compare(strings.ToLower(replyStr), "ok") != 0 {
		resp.Header.Code = int64(alarmpb2.AlarmResponseCode_ALARM_ERROR)
		resp.Header.Error = err.Error()
		return
	}

	return
}

func (s *Server) handleAppNotAlive(header *alarmpb2.RequestHeader, req *alarmpb2.AppNotAliveRequest) (*alarmpb2.AlarmResponse, error) {

}

func (s *Server) handleGatewaySlowLog(header *alarmpb2.RequestHeader, req *alarmpb2.GatewaySlowLogRequest) (*alarmpb2.AlarmResponse, error) {

}

func (s *Server) handleGatewayErrorLog(header *alarmpb2.RequestHeader, req *alarmpb2.GatewayErrorLogRequest) (*alarmpb2.AlarmResponse, error) {

}


