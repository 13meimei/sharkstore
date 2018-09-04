package alarm2

import (
	"time"
	"context"

	"util/log"
)

const (
	ALARMROLE_SYSTEM_ADMIN 		= "system_admin"
	ALARMROLE_CLUSTER_ADMIN 	= "cluster_admin"
	ALARMROLE_CLUSTER_USER 		= "cluster_user"

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

	s.mapAppUpdate(ret)
}

func (s *Server) tableGlobalRulePulling() {
	ret, err := s.getTableGlobalRuleData()
	if err != nil {
		log.Error("pull table global rule failed: %v", err)
		return
	}

	s.mapGlobalRuleUpdate(ret)
}

func (s *Server) tableClusterRulePulling() {
	ret, err := s.getTableClusterRuleData()
	if err != nil {
		log.Error("pull table cluster rule failed: %v", err)
		return
	}

	s.mapClusterRuleUpdate(ret)
}

func (s *Server) tableReceiverPulling() {
	ret, err := s.getTableReceiveData()
	if err != nil {
		log.Error("pull table receiver failed: %v", err)
		return
	}

	s.mapReceiverUpdate(ret)
}

func (s *Server) mapAppUpdate(data []TableApp) {
	if len(data) == 0 {
		log.Warn("no data in table app")
		return
	}

	newMap := make(appMap)
	for _, d := range data {
		if _, ok := newMap[d.clusterId]; !ok {
			newMap[d.clusterId] = make(appClusterMap)
		}
		dd := newMap[d.clusterId]
		dd[d.ipAddr] = d
	}

	s.appLock.Lock()
	defer s.appLock.Unlock()
	s.app = newMap
}

func (s *Server) mapGlobalRuleUpdate(data []TableGlobalRule) {
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

func (s *Server) mapClusterRuleUpdate(data []TableClusterRule) {
	if len(data) == 0 {
		log.Warn("no data in table cluster rule")
		return
	}

	newMap := make(clusterRuleMap)
	for _, d := range data {
		if _, ok := newMap[d.clusterId]; !ok {
			newMap[d.clusterId] = make(clusterRuleNameMap)
		}
		dd := newMap[d.clusterId]
		dd[d.ruleName] = d
	}

	s.globalRuleLock.Lock()
	defer s.globalRuleLock.Unlock()
	s.clusterRule = newMap
}

func (s *Server) mapReceiverUpdate(data []TableReceiver) {
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
	s.receiver = newMap
}
