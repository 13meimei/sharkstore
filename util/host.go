package util

import (
	"net"
	"google.golang.org/grpc/peer"
	"golang.org/x/net/context"
)

func GetLocalIps() []string {
	addrs, err := net.InterfaceAddrs()
    
	if err != nil {
		return nil
	}
    	var ips []string
	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ips = append(ips, ipnet.IP.String())
			}
		}
	}
	return ips
}

func GetIpFromContext(ctx context.Context) string {
	if client, ok := peer.FromContext(ctx); ok{
		return client.Addr.String()
	}
	return ""
}