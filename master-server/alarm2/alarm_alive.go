package alarm2

import (
	"fmt"
	"time"
	"context"

	"github.com/gomodule/redigo/redis"

	"util/log"
	"model/pkg/alarmpb2"
)


func (s *Server) aliveChecking() {
	ctx, cancel := context.WithCancel(s.context)
	defer cancel()
	duration := s.conf.AppAliveCheckingDurationSec
	t := time.NewTimer(duration)
	for {
		select {
		case <-t.C:
			var apps []TableApp
			for clusterId, clusterApps := range s.getMapApp() {
				for ipAddr, app := range clusterApps {
					key, err := newCacheKey(ALARMRULE_APP_NOTALIVE, app.processName, app.clusterId, app.ipAddr)
					if err != nil {
						log.Error("new alive key faileld: process name[%v] cluster id[%v] ip addr[%v]: %v",
							app.processName, app.clusterId, app.ipAddr, err)
						continue
					}

					reply, err := s.jimCommand("exists", key)
					if err != nil {
						log.Error("jim exists command error: %v", err)
						continue
					}
					replyInt, err := redis.Int(reply, err)
					if err != nil {
						log.Error("jim command exists reply type is not int: %v", err)
						continue
					}

					if replyInt != 0 { // key exists
						log.Info("alive key exists: %v", key)
						continue
					}

					log.Warn("app not alive: cluster id[%v] ip addr[%v] app name[%v]", clusterId, ipAddr, app.processName)
					apps = append(apps, app)
				}
			}

			if len(apps) != 0 {
				for _, app := range apps {
					s.handleRuleAlarm(&alarmpb2.RequestHeader{
						ClusterId: app.clusterId,
						IpAddr: app.ipAddr,
					}, &alarmpb2.RuleAlarmRequest{
						RuleName: ALARMRULE_APP_NOTALIVE,
						AlarmValue: 1,
						CmpType: alarmpb2.AlarmValueCompareType_GREATER_THAN,
						Remark: []string{fmt.Sprintf("app name: %v", app.processName)},
					})
				}
			}

			t.Reset(duration)
		case <-ctx.Done():
			return
		}
	}
}
