package ttlcache

import (
	"sync"
	"time"
)

type lruNode struct {
	obj     interface{}
	createtime time.Time
	dietime time.Time
}

type TTLCache struct {
	lock    sync.RWMutex
	timeout time.Duration
	cache   map[interface{}]*lruNode
}

func NewTTLCache(timeout time.Duration) *TTLCache {
	return &TTLCache{
		timeout: timeout,
		cache:   make(map[interface{}]*lruNode),
	}
}

func (self *TTLCache) Put(key interface{}, obj interface{}) {
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

func (self *TTLCache) get(key interface{}) (*lruNode, bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	node, ok := self.cache[key]
	if ok {
		return node, true
	}
	return nil, false
}

func (self *TTLCache) GetAll() []interface{} {
	var values []interface{}
	self.lock.RLock()
	defer self.lock.RUnlock()
	for key, node := range self.cache {
		if node.dietime.After(time.Now()) {
			values = append(values, node.obj)
		}else {
			delete(self.cache, key)
		}
	}
	return values
}

func (self *TTLCache) Get(key interface{}) (interface{}, bool) {
	node, ok := self.get(key)
	if ok {
		if node.dietime.After(time.Now()) {
			return node.obj, true
		}
		return func () (interface{}, bool){
			self.lock.Lock()
			defer self.lock.Unlock()
			node, ok := self.cache[key]
			if ok {
				if node.dietime.After(time.Now()) {
					return node.obj, true
				}
			}
			delete(self.cache, key)
			return nil, false
		}()
	}
	return nil, false
}

func (self *TTLCache) Delete(key interface{}) {
	var find bool
	self.lock.Lock()
	defer self.lock.Unlock()
	if _, find = self.cache[key]; find {
		delete(self.cache, key)
	}
}
