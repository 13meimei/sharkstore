package alarm

import (
	"fmt"
	"errors"
	"strconv"
	"strings"
	"encoding/json"
	"time"
	"net"
	"util/log"
	"github.com/gomodule/redigo/redis"
)

func (s *Server) parseGwAddr(addr string) ([]string, int64, error) {
	gwAddrInfo := strings.SplitN(addr, ":", 2)
	if len(gwAddrInfo) != 2 {
		return nil, 0, errors.New("splitN failed != input arg N")
	}
	gwHost:= gwAddrInfo[0]
	_, err := strconv.ParseInt(gwAddrInfo[1], 10, 64)
	gwPort, err := strconv.ParseInt(gwAddrInfo[1], 10, 64)
	if err != nil {
		return nil, 0, err
	}

	ips, err := net.LookupHost(gwHost)
	if err != nil {
		return nil, 0, err
	}

	return ips, gwPort, nil
}

func (s *Server) loadAliveCheckingAppAddrs() error {
	log.Debug("load cluster info from sharkstore cluster table ")
	clusterInfos, err := s.getClusterInfo()
	if err != nil {
		log.Error("alive checking get cluster info failed: %v", err)
		return err
	}

	for _, info := range clusterInfos {
		var gwAddrs []string

		log.Debug("info row: %v", info)
		if len(info.remark) == 0 || strings.Compare(info.remark, "NULL") == 0 {
			// do with gw domain
			log.Debug("parse gateway host: %v", info.appGwAddr)
			gwIps, gwPort, err := s.parseGwAddr(info.appGwAddr)
			if err != nil {
				log.Error("parse gateway domain failed: %v", err)
				continue
			}

			log.Debug("parsed gateway host result: %v, port: %v", gwIps, gwPort)
			for _, gwIp := range gwIps {
				gwAddrs = append(gwAddrs, fmt.Sprintf("%v:%v", gwIp, gwPort))
			}
		} else {
			// according ipAddrs in remark
			log.Debug("len info remark: %v", info.remark)

			var remark clusterRemark
			if err := json.Unmarshal([]byte(info.remark), &remark); err != nil {
				log.Error("remark [%v] unmarshal failed: %v", info.remark, err)
				continue
			}

			if remark.GatewayAddrsAlarmEnable {
				for _, gwAddr := range remark.GatewayAddrs {
					gwAddrs = append(gwAddrs, gwAddr)
				}
			}
		}

		// add checking list
		for _, gwAddr := range gwAddrs {
			s.addAliveCheckingAppAddr(APP_NAME_GATEWAY, fmt.Sprint(info.id), gwAddr)
		}
	}

	return nil
}

func (s *Server) aliveCheckingAlarm() {
	log.Info("alive checking alarm processing...")

	loadTicker := time.NewTicker(30 *time.Second)
	checkTicker := time.NewTicker(10 *time.Second)
	for {
		select {
		case <-loadTicker.C:
			if err := s.loadAliveCheckingAppAddrs(); err != nil {
				log.Error("loadAliveCheckingAppAddrs failed: %v", err)
			}

		case <-checkTicker.C:
			appKeys := s.swapAliveCheckingAppKeys()
			for _, key := range appKeys {
				log.Debug("to check alive app key: %v", key)
				// get app key from jimdb, if it is not in, then alarm
				// exists return interger
				reply, err := s.jimSendCommand("exists", key)
				if err != nil {
					log.Error("jim send command error: %v", err)
					continue
				}
				replyInt, err := redis.Int(reply, err)
				if err != nil {
					log.Error("jim command setex reply type is not int: %v", err)
					continue
				}

				if replyInt != 0 { // app key exists
					log.Info("alive app key exists: %v", key)
					continue
				}

				var samples []*Sample
				appName, clusterId, appAddr := key.splitAliveAppKey()
				info := make(map[string]interface{})
				info["spaceId"] = clusterId
				info["ip"] = appAddr
				info["app_is_not_alive"] = 1
				info["app_name"] = appName

				samples = append(samples, NewSample("", 0, 0, info))
				samplesJson := SamplesToJson(samples)

				log.Debug("alive alarm samples json: %v", samplesJson)
				if err := s.gateway.notify(Message{
					ClusterId: func() int64 {
						ret, _ :=strconv.ParseInt(clusterId, 10, 64)
						return ret
					}(),
					Title: "alive alarm",
					Content: "",
					samples: samplesJson,
				}, time.Second); err != nil {
					log.Error("alive alarm notify timeout")
				}
			}
		}
	}
}

func (s *Server) addAliveCheckingAppAddr(appName, clusterId, appAddr string) {
	appKey := newAliveAppKey(appName, clusterId, appAddr)
	if len(appKey) == 0 {
		log.Warn("app name [%v] can not be generated", appName)
		return
	}

	s.aliveCheckingLock.Lock()
	defer s.aliveCheckingLock.Unlock()

	s.aliveCheckingAppKeys = append(s.aliveCheckingAppKeys, appKey)
}

func (s *Server) swapAliveCheckingAppKeys() (ret []aliveAppKey) {
	s.aliveCheckingLock.Lock()
	defer s.aliveCheckingLock.Unlock()

	if len(s.aliveCheckingAppKeys) == 0 {
		return
	}

	ret = s.aliveCheckingAppKeys
	s.aliveCheckingAppKeys = nil
	return
}