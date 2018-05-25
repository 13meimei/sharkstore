package safemap

import (
	"sync"
	"errors"
)

var (
	ErrEntryExist       = errors.New("entry exist")
	ErrEntryNotExist    = errors.New("entry not exist")
)

type SafeMap struct {
	lock      sync.RWMutex
	container map[interface{}](interface{})
}

func NewSafeMap() *SafeMap {
	conn := make(map[interface{}](interface{}))
	if conn != nil {
		return &SafeMap{container: conn}
	}
	return nil
}

// all kv pair count
func (this *SafeMap) Len() int {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return len(this.container)
}

// if key exist update, else insert
func (this *SafeMap) Replace(key interface{}, value interface{}) (interface{}, bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	temp, find := this.container[key]
	this.container[key] = value
	return temp, find
}

// if key exist return err, else return nil
func (this *SafeMap) Insert(key interface{}, value interface{}) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	_, find := this.container[key]
	if find {
		return ErrEntryExist
	}
	this.container[key] = value
	return nil
}

// if key exist return nil, else return err
func (this *SafeMap) Update(key interface{}, value interface{}) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	_, find := this.container[key]
	if !find {
		return ErrEntryNotExist
	}
	this.container[key] = value
	return nil
}

// if key exist return value + true, else return nil + false
func (this *SafeMap) Delete(key interface{}) (interface{}, bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	value, find := this.container[key]
	delete(this.container, key)
	return value, find
}

// if key exist return value + true, else return nil + false
func (this *SafeMap) Find(key interface{}) (interface{}, bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	value, find := this.container[key]
	return value, find
}

// walk through all the key value node then callback
func (this *SafeMap) Walk(callback func(k, v interface{})) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	for key, value := range this.container {
		callback(key, value)
	}
}

func (this *SafeMap) BoundedWalk(callback func(k, v interface{}) bool ) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	for key, value := range this.container {
		if callback(key, value) {
			break
		}
	}
}

// clear all the data
func (this *SafeMap) Clear() {
	this.lock.Lock()
	defer this.lock.Unlock()
	if len(this.container) > 0 {
		this.container = make(map[interface{}](interface{}))
	}
}
