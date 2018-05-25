package common

import (
	"crypto/md5"
	"fmt"
)

const (
	// MASTER_CLUSTER_FIXED_ID    = 1
	CLUSTER_FIXED_TOKEN = "test"

)

// 集群1与其他集群签名规则一致
func BuildNewClusterToken(clusterId int, clusterName string, cToken string) string {
	//if clusterId == MASTER_CLUSTER_FIXED_ID {
	//	return Md5Sum(CLUSTER_FIXED_TOKEN_SEED)
	//} else {
	//	return Md5Sum(fmt.Sprintf("%d%s%s", clusterId, clusterName, CLUSTER_FIXED_TOKEN))
	//}
	return CLUSTER_FIXED_TOKEN
}

func CalcMsReqSign(cId int, cToken string, ts int64) string {
	//if cId == MASTER_CLUSTER_FIXED_ID {
	//	return Md5Sum(fmt.Sprintf("%s%d", MASTER_CLUSTER_FIXED_TOKEN, ts))
	//} else {
		return Md5Sum(fmt.Sprintf("%d%d%s", cId, ts, cToken))
	//}
}

func Md5Sum(data string) string {
	if data == "" {
		return ""
	}

	d := []byte(data)
	s := md5.Sum(d)
	return fmt.Sprintf("%x", s)
}
