package ping

import (
	"fmt"
	"time"
	"net"
	"os"
	"path/filepath"

	"master-server/alarm2"
	"util/log"
)

var alarmClient *alarm2.Client

func getAppInfo() (app string, ip []string) {
	app = filepath.Base(os.Args[0])

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return app, ip
	}
	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip = append(ip, ipnet.IP.String())
				continue
			}
		}
	}
	return app, ip
}

func Ping(alarmServerAddr string, clusterId int64, appPort int, ping_interval int64) {
	t := time.NewTicker(time.Duration(ping_interval) * time.Second)
	for {
		select {
		case <-t.C:
			if alarmClient == nil {
				var err error
				alarmClient, err = alarm2.NewAlarmClient2(alarmServerAddr)
				if err != nil {
					log.Error("new alarm client failed: %v", err)
					continue
				}
			}
			appName, appIps := getAppInfo()
			if len(appIps) == 0 {
				log.Error("get app ips len is 0")
				continue
			}
			appAddr := fmt.Sprintf("%v:%v", appIps[0], appPort)
			if err := alarmClient.AlarmAppHeartbeat(clusterId, appAddr, appName, ping_interval); err != nil {
				log.Error("do alarm app heartbeat failed: %v", err)
				continue
			}
		}
	}
}
