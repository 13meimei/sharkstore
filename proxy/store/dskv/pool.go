package dskv

import "sync"

var proxyPool *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return &KvProxy{}
	},
}

func GetKvProxy() *KvProxy {
	return proxyPool.Get().(*KvProxy)
}

func PutKvProxy(proxy *KvProxy) {
	if proxy == nil {
		return
	}
	proxy.Reset()
	proxyPool.Put(proxy)
}

var requestPool *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return &Request{}
	},
}

func GetRequest() *Request {
	return requestPool.Get().(*Request)
}

func PutRequest(req *Request) {
	if req == nil {
		return
	}
	req.Reset()
	requestPool.Put(req)
}

