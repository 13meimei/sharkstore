package common

import (
	"crypto/md5"
	"fmt"
	"strings"
	"encoding/base64"
)

const (
	MASTER_CLUSTER_FIXED_ID = 1
	CLUSTER_FIXED_TOKEN = "test"
)

func BuildNewClusterToken(clusterId int, cToken string) string {
	if clusterId == MASTER_CLUSTER_FIXED_ID || len(cToken) == 0{
		return CLUSTER_FIXED_TOKEN
	}
	return cToken
}

func CalcMsReqSign(cId int, cToken string, ts int64) string {
	cToken = BuildNewClusterToken(cId, cToken)
	return Md5Sum(fmt.Sprintf("%d%d%s", cId, ts, cToken))
}

func Md5Sum(data string) string {
	if data == "" {
		return ""
	}

	d := []byte(data)
	s := md5.Sum(d)
	return fmt.Sprintf("%x", s)
}

var(
	base64Table        = "ABCDEFGHIJKLMNXYZabcdefghijklmnopqrstuvw0123456789^%$(+/*&<>!-@)"
	hashFunctionHeader = "s.j.c"
	hashFunctionFooter = "08.09.21.16.64"
)

/**
 * base64加密
 */
func Base64Encode(str string) string {
	var src []byte = []byte(hashFunctionHeader + str + hashFunctionFooter)
	coder := base64.NewEncoding(base64Table)
	return coder.EncodeToString(src)
}

/**
 * base64解密
 */
func Base64Decode(str string) (string, error) {
	coder := base64.NewEncoding(base64Table)
	by, err := coder.DecodeString(str)
	if err != nil {
		return str, err
	}
	return strings.Replace(strings.Replace(string(by), hashFunctionHeader, "", -1), hashFunctionFooter, "", -1), nil
}