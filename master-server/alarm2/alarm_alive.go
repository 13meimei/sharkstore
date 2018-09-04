package alarm2

import (
	"fmt"
	"strings"
	"strconv"
	"errors"
)

type aliveKey string



func (key aliveKey) splitAliveKey() (appName string, clusterId int64, appAddr string, err error) {
	strs := strings.Split(string(key), ALIVEKEY_JOIN_LETTER)
	appName = strs[1]
	clusterIdStr := strs[2]
	appAddr = strs[3]

	clusterId, err = strconv.ParseInt(clusterIdStr, 10, 64)
	return
}

// eg. alive_gateway_1_127.0.0.1:8080
func newAliveKey(appName string, clusterId int64, appAddr string) (key aliveKey, err error) {
	name := strings.ToLower(appName)
	switch {
	case strings.HasPrefix(name, ALIVEKEY_GATEWAY):
		key = aliveKey(fmt.Sprintf("alive%s%v%s%v%s%v",
			ALIVEKEY_JOIN_LETTER, ALIVEKEY_GATEWAY,
			ALIVEKEY_JOIN_LETTER, clusterId,
			ALIVEKEY_JOIN_LETTER, appAddr))
	case strings.HasPrefix(name, ALIVEKEY_MASTER):
		key = aliveKey(fmt.Sprintf("alive%s%v%s%v%s%v",
			ALIVEKEY_JOIN_LETTER, ALIVEKEY_MASTER,
			ALIVEKEY_JOIN_LETTER, clusterId,
			ALIVEKEY_JOIN_LETTER, appAddr))
	case strings.HasPrefix(name, ALIVEKEY_METRIC):
		key = aliveKey(fmt.Sprintf("alive%s%v%s%v%s%v",
			ALIVEKEY_JOIN_LETTER, ALIVEKEY_METRIC,
			ALIVEKEY_JOIN_LETTER, clusterId,
			ALIVEKEY_JOIN_LETTER, appAddr))
	default:
		err = errors.New("unknown appname prefix")
	}
	return
}