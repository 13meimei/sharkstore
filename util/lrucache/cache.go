package lrucache

import (
	"sync"
	"time"
)

type lruNode struct {
	obj     interface{}
	createtime time.Time
	dietime time.Time
}

type LRUCache struct {
	timeout time.Duration
	cache   map[string]*lruNode
	lock    sync.RWMutex
}

func NewLRUCache(timeout time.Duration) *LRUCache {
	return &LRUCache{
		timeout: timeout,
		cache:   make(map[string]*lruNode),
	}
}

func (self *LRUCache) Put(key string, obj interface{}) {
	self.lock.Lock()
	defer self.lock.Unlock()
	var node *lruNode
	var find bool
	now := time.Now()
	if node, find = self.cache[key]; find {
		//未超时
		if node.dietime.After(time.Now()) {
			node.dietime = now.Add(self.timeout)
			node.obj = obj
		}
	}
	self.cache[key] = &lruNode{
		obj:     obj,
		dietime: now.Add(self.timeout),
		createtime: now,
	}
}

func (self *LRUCache) Get(key string) (interface{}, time.Time, bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()

	node, ok := self.cache[key]
	if ok {
		if node.dietime.After(time.Now()) {
			return node.obj, node.createtime, true
		}

		delete(self.cache, key)
	}
	return nil, time.Time{}, false
}