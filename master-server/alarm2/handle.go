package alarm2

import (
	"errors"

	"util/log"
	"model/pkg/alarmpb2"
	"time"
)

func (s *Server) handleAppHeartbeat(header *alarmpb2.RequestHeader, req *alarmpb2.AppHeartbeatRequest) (resp *alarmpb2.AlarmResponse, err error) {
	log.Debug("handle app heartbeat request: %v", req)
	resp = new(alarmpb2.AlarmResponse)

	// eg. app_not_alive
	aliveKey, err := encodeCacheKey(cacheKey{
		ALARMRULE_APP_NOTALIVE,
		req.GetAppName(),
		header.GetClusterId(),
		header.GetIpAddr()})
	if err != nil {
		resp.Header.Code = int64(alarmpb2.AlarmResponseCode_ALARM_ERROR)
		resp.Header.Error = err.Error()
		return
	}

	// setex key with ttl ping_interval to jimdb
	if err := s.cacheOpImpl.setex(aliveKey, "", req.GetHbIntervalTime()*2); err != nil {
		resp.Header.Code = int64(alarmpb2.AlarmResponseCode_ALARM_ERROR)
		resp.Header.Error = err.Error()
		return resp, err
	}

	return
}

func (s *Server) handleRuleAlarm(header *alarmpb2.RequestHeader, req *alarmpb2.RuleAlarmRequest) (*alarmpb2.AlarmResponse, error) {
	var r Rule

	// get global rule
	gRule := s.getMapGlobalRule()
	if len(gRule) == 0 {
		return nil, errors.New("no global rule")
	}
	if gr, ok := gRule[req.GetRuleName()]; !ok {
		return nil, errors.New("unknown rule")
	} else {
		r = gr.Rule
	}

	// get cluster rule
	cRule := s.getMapClusterRule()
	if len(cRule) != 0 {
		if crs, ok := cRule[header.GetClusterId()]; ok {
			if cr, ok := crs[req.GetRuleName()]; ok {
				r = cr.Rule
			}
		}
	}

	// enable?
	log.Debug("rule alarm: cluster id[%v] rule name[%v] enable[%v]",
		header.GetClusterId(), req.GetRuleName(), r.enable)
	if r.enable == 0 {
		return &alarmpb2.AlarmResponse{}, nil
	}

	// threshold
	var threshold bool = false

	switch req.GetCmpType() {
	case alarmpb2.AlarmValueCompareType_EQUAL:
		log.Debug("rule alarm: cluster id[%v] ip addr[%v] rule name[%v] alarm value[%v] == threshold[%v]",
			header.GetClusterId(), header.GetIpAddr(), req.GetRuleName(), req.GetAlarmValue(), r.threshold)
		if req.GetAlarmValue() == r.threshold {
			threshold = true
		}
	case alarmpb2.AlarmValueCompareType_GREATER_THAN:
		log.Debug("rule alarm: cluster id[%v] ip addr[%v] rule name[%v] alarm value[%v] > threshold[%v]",
			header.GetClusterId(), header.GetIpAddr(), req.GetRuleName(), req.GetAlarmValue(), r.threshold)
		if req.GetAlarmValue() > r.threshold {
			threshold = true
		}
	case alarmpb2.AlarmValueCompareType_LESS_THAN:
		log.Debug("rule alarm: cluster id[%v] ip addr[%v] rule name[%v] alarm value[%v] < threshold[%v]",
			header.GetClusterId(), header.GetIpAddr(), req.GetRuleName(), req.GetAlarmValue(), r.threshold)
		if req.GetAlarmValue() < r.threshold {
			threshold = true
		}
	case alarmpb2.AlarmValueCompareType_NOT_EQUAL:
		log.Debug("rule alarm: cluster id[%v] ip addr[%v] rule name[%v] alarm value[%v] != threshold[%v]",
			header.GetClusterId(), header.GetIpAddr(), req.GetRuleName(), req.GetAlarmValue(), r.threshold)
		if req.GetAlarmValue() != r.threshold {
			threshold = true
		}
	case alarmpb2.AlarmValueCompareType_NOT_GREATER_THAN:
		log.Debug("rule alarm: cluster id[%v] ip addr[%v] rule name[%v] alarm value[%v] <= threshold[%v]",
			header.GetClusterId(), header.GetIpAddr(), req.GetRuleName(), req.GetAlarmValue(), r.threshold)
		if req.GetAlarmValue() <= r.threshold {
			threshold = true
		}
	case alarmpb2.AlarmValueCompareType_NOT_LESS_THAN:
		log.Debug("rule alarm: cluster id[%v] ip addr[%v] rule name[%v] alarm value[%v] >= threshold[%v]",
			header.GetClusterId(), header.GetIpAddr(), req.GetRuleName(), req.GetAlarmValue(), r.threshold)
		if req.GetAlarmValue() >= r.threshold {
			threshold = true
		}
	default:
		return &alarmpb2.AlarmResponse{
			Header: &alarmpb2.ResponseHeader{
				//Code: ,
				Error: "unknown compare type",
			},
		}, nil
	}

	if threshold == false {
		return &alarmpb2.AlarmResponse{}, nil
	}

	// get key cache
	ruleKey, err := encodeCacheKey(cacheKey{
		req.GetRuleName(),
		header.GetAppName(),
		header.GetClusterId(),
		header.GetIpAddr()})
	if err != nil {
		return nil, err
	}
	reply, err := s.cacheOpImpl.get(ruleKey)
	if err != nil {
		// treat as key not exists
		log.Warn("jim get command failed: %v, treat as key[%v] not exists", err, ruleKey)

		curTime := time.Now().Unix()
		var ruleValue = cacheValue{
			TriggerTime: curTime,
			Count: 1,
		}

		ruleValueStr, err := encodeCacheValue(ruleValue)
		if err != nil {
			return nil, err
		}

		// expire time = r.durable
		err = s.cacheOpImpl.setex(ruleKey, ruleValueStr, r.durable)
		if err != nil {
			return nil, err
		}
	} else {
		ruleValue, err := decodeCacheValue(reply)
		if err != nil {
			return nil, err
		}
		curTime := time.Now().Unix()
		if ruleValue.TriggerTime <= curTime {
			ruleValue.Count++
		}

		if ruleValue.Count >= r.count {
			// append report
			s.ruleAlarmReportAppend(header.GetClusterId(), header.GetAppName(), header.GetIpAddr(), req, r.threshold)

			ruleValue.TriggerTime = curTime + r.interval
			ruleValue.Count = 0

			ruleValueStr, err := encodeCacheValue(ruleValue)
			if err != nil {
				return nil, err
			}

			// expire time = r.interval + r.durable
			s.cacheOpImpl.setex(ruleKey, ruleValueStr, r.interval + r.durable)
		}
	}
	return &alarmpb2.AlarmResponse{}, nil
}

//func (s *Server) handleAppNotAlive(header *alarmpb2.RequestHeader, req *alarmpb2.AppNotAliveRequest) (*alarmpb2.AlarmResponse, error) {
//	return &alarmpb2.AlarmResponse{}, nil
//}
//
//func (s *Server) handleGatewaySlowLog(header *alarmpb2.RequestHeader, req *alarmpb2.GatewaySlowLogRequest) (*alarmpb2.AlarmResponse, error) {
//	return &alarmpb2.AlarmResponse{}, nil
//}
//
//func (s *Server) handleGatewayErrorLog(header *alarmpb2.RequestHeader, req *alarmpb2.GatewayErrorLogRequest) (*alarmpb2.AlarmResponse, error) {
//	return &alarmpb2.AlarmResponse{}, nil
//}

