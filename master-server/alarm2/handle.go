package alarm2

import (
	"time"

	"model/pkg/alarmpb2"

	"util/log"
)

func (s *Server) handleAppHeartbeat(header *alarmpb2.RequestHeader, req *alarmpb2.AppHeartbeatRequest) (resp *alarmpb2.AlarmResponse, err error) {
	log.Debug("handle app heartbeat request: %v", req)
	resp = &alarmpb2.AlarmResponse{
		Header: &alarmpb2.ResponseHeader{},
	}

	// eg. app_not_alive
	aliveKey, err := encodeCacheKey(cacheKey{
		ALARMRULE_APP_NOTALIVE,
		header.GetAppName(),
		header.GetClusterId(),
		header.GetIpAddr()})
	if err != nil {
		resp.Header.Code = alarmpb2.AlarmResponseCode_ERROR
		resp.Header.Error = err.Error()
		return
	}

	// setex key with ttl ping_interval to jimdb
	vStr, _ := encodeCacheValue(cacheValue{})
	if err = s.cacheOpImpl.setex(aliveKey, vStr, req.GetHbIntervalTime()*2); err != nil {
		resp.Header.Code = alarmpb2.AlarmResponseCode_ERROR
		resp.Header.Error = err.Error()
		return
	}

	return
}

func (s *Server) handleRuleAlarm(header *alarmpb2.RequestHeader, req *alarmpb2.RuleAlarmRequest) (resp *alarmpb2.AlarmResponse, err error) {
	log.Debug("handle rule alarm request: %v", req)
	resp = &alarmpb2.AlarmResponse{
		Header: &alarmpb2.ResponseHeader{},
	}

	var r Rule

	// get global rule
	gRule := s.getMapGlobalRule()
	if len(gRule) == 0 {
		resp.Header.Code = alarmpb2.AlarmResponseCode_ERROR
		resp.Header.Error = "no global rule"
		return
	}
	gr, ok := gRule[req.GetRuleName()]
	if !ok {
		resp.Header.Code = alarmpb2.AlarmResponseCode_ERROR
		resp.Header.Error = "unknown rule"
		return
	}
	r = gr.Rule
	log.Debug("rule alarm cluster id[%v] get global rule : %+v", header.GetClusterId(), r)

	// get cluster rule
	cRule := s.getMapClusterRule()
	if len(cRule) != 0 {
		if crs, ok := cRule[header.GetClusterId()]; ok {
			if cr, ok := crs[req.GetRuleName()]; ok {
				r = cr.Rule
				log.Debug("rule alarm cluster id[%v] get cluster rule: %+v", header.GetClusterId(), r)
			}
		}
	}

	// enable?
	if r.Enable == 0 {
		log.Info("rule alarm cluster id[%v] rule name[%v] not enable", header.GetClusterId(), r.Name)
		return
	}

	// threshold
	var thresholdJudge bool = false

	switch req.GetCmpType() {
	case alarmpb2.AlarmValueCompareType_EQUAL:
		log.Debug("rule alarm: cluster id[%v] ip addr[%v] rule name[%v] alarm value[%v] == threshold[%v]",
			header.GetClusterId(), header.GetIpAddr(), req.GetRuleName(), req.GetAlarmValue(), r.Threshold)
		if req.GetAlarmValue() == r.Threshold {
			thresholdJudge = true
		}
	case alarmpb2.AlarmValueCompareType_GREATER_THAN:
		log.Debug("rule alarm: cluster id[%v] ip addr[%v] rule name[%v] alarm value[%v] > threshold[%v]",
			header.GetClusterId(), header.GetIpAddr(), req.GetRuleName(), req.GetAlarmValue(), r.Threshold)
		if req.GetAlarmValue() > r.Threshold {
			thresholdJudge = true
		}
	case alarmpb2.AlarmValueCompareType_LESS_THAN:
		log.Debug("rule alarm: cluster id[%v] ip addr[%v] rule name[%v] alarm value[%v] < threshold[%v]",
			header.GetClusterId(), header.GetIpAddr(), req.GetRuleName(), req.GetAlarmValue(), r.Threshold)
		if req.GetAlarmValue() < r.Threshold {
			thresholdJudge = true
		}
	case alarmpb2.AlarmValueCompareType_NOT_EQUAL:
		log.Debug("rule alarm: cluster id[%v] ip addr[%v] rule name[%v] alarm value[%v] != threshold[%v]",
			header.GetClusterId(), header.GetIpAddr(), req.GetRuleName(), req.GetAlarmValue(), r.Threshold)
		if req.GetAlarmValue() != r.Threshold {
			thresholdJudge = true
		}
	case alarmpb2.AlarmValueCompareType_NOT_GREATER_THAN:
		log.Debug("rule alarm: cluster id[%v] ip addr[%v] rule name[%v] alarm value[%v] <= threshold[%v]",
			header.GetClusterId(), header.GetIpAddr(), req.GetRuleName(), req.GetAlarmValue(), r.Threshold)
		if req.GetAlarmValue() <= r.Threshold {
			thresholdJudge = true
		}
	case alarmpb2.AlarmValueCompareType_NOT_LESS_THAN:
		log.Debug("rule alarm: cluster id[%v] ip addr[%v] rule name[%v] alarm value[%v] >= threshold[%v]",
			header.GetClusterId(), header.GetIpAddr(), req.GetRuleName(), req.GetAlarmValue(), r.Threshold)
		if req.GetAlarmValue() >= r.Threshold {
			thresholdJudge = true
		}
	default:
		resp.Header.Code = alarmpb2.AlarmResponseCode_ERROR
		resp.Header.Error = "unknown compare type"
		return
	}

	if thresholdJudge == false {
		log.Info("rule alarm cluster id[%v] rule name[%v] threshold judge false", header.GetClusterId(), r.Name)
		return
	}

	// get key cache
	ruleKey, err := encodeCacheKey(cacheKey{
		req.GetRuleName(),
		header.GetAppName(),
		header.GetClusterId(),
		header.GetIpAddr()})
	if err != nil {
		resp.Header.Code = alarmpb2.AlarmResponseCode_ERROR
		resp.Header.Error = err.Error()
		return
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
			resp.Header.Code = alarmpb2.AlarmResponseCode_ERROR
			resp.Header.Error = err.Error()
			return resp, err
		}

		// expire time = r.durable
		err = s.cacheOpImpl.setex(ruleKey, ruleValueStr, r.Durable)
		if err != nil {
			resp.Header.Code = alarmpb2.AlarmResponseCode_ERROR
			resp.Header.Error = err.Error()
			return resp, err
		}
	} else {
		ruleValue, err := decodeCacheValue(reply)
		if err != nil {
			resp.Header.Code = alarmpb2.AlarmResponseCode_ERROR
			resp.Header.Error = err.Error()
			return resp, err
		}
		curTime := time.Now().Unix()
		if ruleValue.TriggerTime <= curTime {
			ruleValue.Count++
			log.Debug("rule alarm cluster id[%v] rule name[%v] ip addr[%v] trigger time[%v] count[%v]",
				header.GetClusterId(), r.Name, header.GetIpAddr(), ruleValue.TriggerTime, ruleValue.Count)
		}

		if ruleValue.Count >= r.Count {
			log.Debug("rule alarm cluster id[%v] app name[%v] ip addr[%v] rule name[%v] report append",
				header.GetClusterId(), header.GetAppName(), header.GetIpAddr(), r.Name)
			// append report
			s.ruleAlarmReportAppend(header.GetClusterId(), header.GetAppName(), header.GetIpAddr(), req, r.Threshold)

			ruleValue.TriggerTime = curTime + r.Interval
			ruleValue.Count = 0

			ruleValueStr, err := encodeCacheValue(ruleValue)
			if err != nil {
				resp.Header.Code = alarmpb2.AlarmResponseCode_ERROR
				resp.Header.Error = err.Error()
				return resp, err
			}

			// expire time = r.interval + r.durable
			s.cacheOpImpl.setex(ruleKey, ruleValueStr, r.Interval + r.Durable)
		}
	}
	return
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

