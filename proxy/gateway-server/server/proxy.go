package server

import (
	dsClient "pkg-go/ds_client"
	msClient "pkg-go/ms_client"
	"sync"

	"golang.org/x/net/context"
)

type ProxyConfig struct {
	AggrEnable  bool
	MaxLimit    uint64
	Performance PerformConfig
}

type Proxy struct {
	config *ProxyConfig

	msCli  msClient.Client
	dsCli  dsClient.KvClient
	router *Router

	maxWorkNum  uint64
	taskQueues  []chan Task
	workRecover chan int
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewProxy(msAddrs []string, config *ProxyConfig) *Proxy {
	msCli, err := msClient.NewClient(msAddrs)
	if err != nil {
		return nil
	}
	router := NewRouter(msCli)
	if router == nil {
		return nil
	}
	var taskQueues []chan Task
	for i := 0; i < int(config.Performance.MaxWorkNum); i++ {
		queue := make(chan Task, config.Performance.MaxTaskQueueLen)
		taskQueues = append(taskQueues, queue)
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := &Proxy{
		router:      router,
		msCli:       msCli,
		dsCli:       dsClient.NewRPCClient(config.Performance.GrpcPoolSize),
		config:      config,
		ctx:         ctx,
		cancel:      cancel,
		maxWorkNum:  config.Performance.MaxWorkNum,
		taskQueues:  taskQueues,
		workRecover: make(chan int, config.Performance.MaxWorkNum),
	}

	for i, queue := range taskQueues {
		proxy.wg.Add(1)
		go proxy.work(i, queue)
	}
	proxy.wg.Add(1)
	go proxy.workMonitor()
	return proxy
}

func (p *Proxy) Close() {
	p.cancel()
	p.wg.Wait()
}
