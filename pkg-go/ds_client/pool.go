package client

import (
	"time"
	"sync"
	"errors"
	"util/log"
)

type createConnFunc func(addr string) (RpcClient, error)

type Pool struct {
	size int64
	pool  []RpcClient
}

func NewPool(size int, addr string, fun createConnFunc) (*Pool, error) {
	var pool []RpcClient
	for i := 0; i < size; i++ {
		cli, err := fun(addr)
		if err != nil {
			return nil, err
		}
		pool = append(pool, cli)
	}
	return &Pool{size: int64(size), pool: pool}, nil
}

func (p *Pool) GetConn() RpcClient {
	index := time.Now().UnixNano() % p.size
	return p.pool[index]
}

func (p *Pool) Close() {
	for _, c := range p.pool {
		c.Close()
	}
}

type ResourcePool struct {
	lock     sync.RWMutex
	size int
	set      map[string]*Pool
}

func NewResourcePool(size int) *ResourcePool {
	return &ResourcePool{size: size, set: make(map[string]*Pool)}
}

func (rp *ResourcePool) Close() {
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	for _, pool := range rp.set {
		pool.Close()
	}
}

func (rp *ResourcePool) GetConn(addr string) (RpcClient, error) {
	if len(addr) == 0 {
		return nil, errors.New("invalid address")
	}
	var pool *Pool
	var ok bool
	var err error
	rp.lock.RLock()
	if pool, ok = rp.set[addr]; ok {
		rp.lock.RUnlock()
		return pool.GetConn(), nil
	}
	rp.lock.RUnlock()
	rp.lock.Lock()
	defer rp.lock.Unlock()
	// 已经建立链接了
	if pool, ok = rp.set[addr]; ok {
		return pool.GetConn(), nil
	}
	pool, err = NewPool(rp.size, addr, func(_addr string) (RpcClient, error) {
		cli := NewDSRpcClient(addr, func(addr string) (*ConnTimeout, error) {
			log.Warn("conn to %s dialTimeout:%d writeTimeout:%d,readTimeout:%d",addr,dialTimeout,WriteTimeout,ReadTimeout)
			conn, err := DialTimeout(addr, dialTimeout)
			if err != nil {
				log.Error("conn to %s err :%s",addr,err.Error())
				return nil, err
			}
			conn.SetWriteTimeout(WriteTimeout)
			conn.SetReadTimeout(ReadTimeout)
			return conn, nil
		})
		return cli, nil
	})
	if err != nil {
		log.Error("new pool[address %s] failed, err[%v]", addr, err)
		return nil, err
	}

	rp.set[addr] = pool
	return pool.GetConn(), nil
}