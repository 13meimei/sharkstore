package alarm2

import (
	"fmt"
	"errors"
	"strings"
	"time"
	"context"
	"net/http"

	"util/log"
	"util/deepcopy"
)

const (
	APPNAME_GATEWAY 		= "gateway"
	APPNAME_MASTER 			= "master"
	APPNAME_METRIC 			= "metric"
	APPNAME_JOIN_LETTER 	= "_"
)

const (
	ALARMROLE_SYSTEM_ADMIN 		= "system_admin"
	ALARMROLE_CLUSTER_ADMIN 	= "cluster_admin"
	ALARMROLE_CLUSTER_USER 		= "cluster_user"

)

const (
	ALARMRULE_APP_NOTALIVE 		= "app_not_alive"
	ALARMRULE_GATEWAY_SLOWLOG 	= "gateway_slowlog"
	ALARMRULE_GATEWAY_ERRORLOG 	= "gateway_errorlog"
)

func (s *Server) timingDbPulling() {
	ctx, cancel := context.WithCancel(s.context)
	defer cancel()
	duration := s.conf.MysqlPullingDurationSec
	t := time.NewTimer(duration)
	for {
		select {
		case <-t.C:
			s.tableAppPulling()
			s.tableGlobalRulePulling()
			s.tableClusterRulePulling()
			s.tableReceiverPulling()

			t.Reset(duration)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) tableAppPulling() {
	ret, err := s.getTableAppData()
	if err != nil {
		log.Error("pull table app failed: %v", err)
		return
	}

	s.updateMapApp(ret)
}

func (s *Server) tableGlobalRulePulling() {
	ret, err := s.getTableGlobalRuleData()
	if err != nil {
		log.Error("pull table global rule failed: %v", err)
		return
	}

	s.updateMapGlobalRule(ret)
}

func (s *Server) tableClusterRulePulling() {
	ret, err := s.getTableClusterRuleData()
	if err != nil {
		log.Error("pull table cluster rule failed: %v", err)
		return
	}

	s.updateMapClusterRule(ret)
}

func (s *Server) tableReceiverPulling() {
	ret, err := s.getTableReceiveData()
	if err != nil {
		log.Error("pull table receiver failed: %v", err)
		return
	}

	s.updateMapReceiver(ret)
}

func (s *Server) updateMapApp(data []TableApp) {
	if len(data) == 0 {
		log.Warn("no data in table app")
		return
	}

	newMap := make(appClusterMap)
	for _, d := range data {
		if _, ok := newMap[d.clusterId]; !ok {
			newMap[d.clusterId] = make(appMap)
		}
		dd := newMap[d.clusterId]
		dd[d.ipAddr] = d
	}

	s.appLock.Lock()
	defer s.appLock.Unlock()
	s.clusterApp = newMap
}

func (s *Server) getMapApp() (ret appClusterMap) {
	s.appLock.RLock()
	defer s.appLock.RUnlock()

	ret = deepcopy.Iface(s.clusterApp).(appClusterMap)
	return
}

func (s *Server) updateMapGlobalRule(data []TableGlobalRule) {
	if len(data) == 0 {
		log.Warn("no data in table global rule")
		return
	}

	newMap := make(globalRuleMap)
	for _, d := range data {
		newMap[d.name] = d
	}

	s.globalRuleLock.Lock()
	defer s.globalRuleLock.Unlock()
	s.globalRule = newMap
}

func (s *Server) getMapGlobalRule() (ret globalRuleMap) {
	s.globalRuleLock.RLock()
	defer s.globalRuleLock.RUnlock()

	ret = deepcopy.Iface(s.globalRule).(globalRuleMap)
	return
}

func (s *Server) updateMapClusterRule(data []TableClusterRule) {
	if len(data) == 0 {
		log.Warn("no data in table cluster rule")
		return
	}

	newMap := make(ruleClusterMap)
	for _, d := range data {
		if _, ok := newMap[d.clusterId]; !ok {
			newMap[d.clusterId] = make(ruleClusterNameMap)
		}
		dd := newMap[d.clusterId]
		dd[d.name] = d
	}

	s.globalRuleLock.Lock()
	defer s.globalRuleLock.Unlock()
	s.clusterRule = newMap
}

func (s *Server) getMapClusterRule() (ret ruleClusterMap) {
	s.clusterRuleLock.RLock()
	defer s.clusterRuleLock.RUnlock()

	ret = deepcopy.Iface(s.clusterRule).(ruleClusterMap)
	return
}

func (s *Server) updateMapReceiver(data []TableReceiver) {
	if len(data) == 0 {
		log.Warn("no data in table receiver")
		return
	}

	newMap := make(receiverClusterMap)
	for _, d := range data {
		if _, ok := newMap[d.clusterId]; !ok {
			newMap[d.clusterId] = make(receiverMap)
		}
		dd := newMap[d.clusterId]
		dd[d.erp] = d
	}

	s.globalRuleLock.Lock()
	defer s.globalRuleLock.Unlock()
	s.clusterReceiver = newMap
}

func (s *Server) getMapReceiver() (ret receiverClusterMap) {
	s.receiverLock.RLock()
	defer s.receiverLock.RUnlock()

	ret = deepcopy.Iface(s.clusterReceiver).(receiverClusterMap)
	return
}

func newCacheKey(ruleName, appName string, clusterId int64, appAddr string) (key string, err error) {
	name := strings.ToLower(appName)
	switch {
	case strings.HasPrefix(name, APPNAME_GATEWAY):
		key = fmt.Sprintf("%s%s%v%s%v%s%v", ruleName,
			APPNAME_JOIN_LETTER, APPNAME_GATEWAY,
			APPNAME_JOIN_LETTER, clusterId,
			APPNAME_JOIN_LETTER, appAddr)
	case strings.HasPrefix(name, APPNAME_MASTER):
		key = fmt.Sprintf("%s%s%v%s%v%s%v", ruleName,
			APPNAME_JOIN_LETTER, APPNAME_MASTER,
			APPNAME_JOIN_LETTER, clusterId,
			APPNAME_JOIN_LETTER, appAddr)
	case strings.HasPrefix(name, APPNAME_METRIC):
		key = fmt.Sprintf("%s%s%v%s%v%s%v", ruleName,
			APPNAME_JOIN_LETTER, APPNAME_METRIC,
			APPNAME_JOIN_LETTER, clusterId,
			APPNAME_JOIN_LETTER, appAddr)
	default:
		err = errors.New("unknown app name")
	}
	return
}

func (s *Server) doReport(msg string) error {
	req, err := http.NewRequest("POST", s.conf.AlarmGatewayAddr, strings.NewReader(msg))
	if err != nil {
		return err
	}
	resp, err := s.reportClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}