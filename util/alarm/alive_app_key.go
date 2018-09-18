package alarm

import (
	"fmt"
	"strings"
	"util/log"
)

type aliveAppKey string

const (
	APP_NAME_GATEWAY = "gateway"
	APP_NAME_MASTER = "master"
	APP_NAME_METRIC = "metric"
	APP_KEY_JOIN_LETTER = "_"
)

func (key aliveAppKey) splitAliveAppKey() (appName, clusterId, appAddr string) {
	strs := strings.Split(string(key), APP_KEY_JOIN_LETTER)
	appName = strs[1]
	clusterId = strs[2]
	appAddr = strs[3]
	return
}

// ex. alive_gateway_1_127.0.0.1:8080
func newAliveAppKey(appName, clusterId, appAddr string) aliveAppKey {
	name := strings.ToLower(appName)
	switch {
	case strings.HasPrefix(name, APP_NAME_GATEWAY):
		log.Info("in gen app key: treat [%v] as [%v]", appName, APP_NAME_GATEWAY)
		return aliveAppKey(fmt.Sprintf("alive%s%v%s%v%s%v",
			APP_KEY_JOIN_LETTER, APP_NAME_GATEWAY,
			APP_KEY_JOIN_LETTER, clusterId,
			APP_KEY_JOIN_LETTER, appAddr))
	case strings.HasPrefix(name, APP_NAME_MASTER):
		log.Info("in gen app key: treat [%v] as [%v]", appName, APP_NAME_MASTER)
		return aliveAppKey(fmt.Sprintf("alive%s%v%s%v%s%v",
			APP_KEY_JOIN_LETTER, APP_NAME_MASTER,
			APP_KEY_JOIN_LETTER, clusterId,
			APP_KEY_JOIN_LETTER, appAddr))
	case strings.HasPrefix(name, APP_NAME_METRIC):
		log.Info("in gen app key: treat [%v] as [%v]", appName, APP_NAME_METRIC)
		return aliveAppKey(fmt.Sprintf("alive%s%v%s%v%s%v",
			APP_KEY_JOIN_LETTER, APP_NAME_METRIC,
			APP_KEY_JOIN_LETTER, clusterId,
			APP_KEY_JOIN_LETTER, appAddr))
	default:
		return ""
	}
}