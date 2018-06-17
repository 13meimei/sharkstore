// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package server

import (
	"bytes"
	"sync"
	"util"
	"fmt"
	"time"

	"model/pkg/metapb"
	"util/log"
	"container/list"
	"github.com/google/btree"
)

type ReplicaCache struct {
	lock        sync.RWMutex
	rangesMap   map[uint64]int
	replicasMap map[uint64]*metapb.Replica
}

func NewReplicaCache() *ReplicaCache {
	return &ReplicaCache{
		replicasMap: make(map[uint64]*metapb.Replica),
		rangesMap:   make(map[uint64]int)}
}

func (rc *ReplicaCache) Add(peer *metapb.Replica) {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	if count, find := rc.rangesMap[peer.GetRangeId()]; find {
		count++
		rc.rangesMap[peer.GetRangeId()] = count
	} else {
		rc.rangesMap[peer.GetRangeId()] = 1
	}
	rc.replicasMap[peer.GetPeer().GetId()] = peer
}

func (rc *ReplicaCache) Delete(id uint64) {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	if peer, find := rc.replicasMap[id]; find {
		if count, find := rc.rangesMap[peer.GetRangeId()]; find {
			count--
			if count == 0 {
				delete(rc.rangesMap, peer.GetRangeId())
			} else {
				rc.rangesMap[peer.GetRangeId()] = count
			}
		}
		delete(rc.replicasMap, id)
	}
}

func (rc *ReplicaCache) FindReplica(id uint64) (*metapb.Replica, bool) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	if peer, find := rc.replicasMap[id]; find {
		return peer, true
	}
	return nil, false
}

func (rc *ReplicaCache) GetAllReplica() []*metapb.Replica {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	var replicas []*metapb.Replica
	for _, peer := range rc.replicasMap {
		replicas = append(replicas, peer)
	}
	return replicas
}

func (rc *ReplicaCache) GetAllRangIds() []uint64 {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	var rangeIds []uint64
	for id, _ := range rc.rangesMap {
		rangeIds = append(rangeIds, id)
	}
	return rangeIds
}

func (rc *ReplicaCache) Size() int {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	return len(rc.replicasMap)
}

type RangeCache struct {
	lock       sync.RWMutex
	rangesMap  map[uint64]*Range
	rangesTree *rangeTree
}

func NewRangeCache() *RangeCache {
	return &RangeCache{rangesMap: make(map[uint64]*Range), rangesTree: newRangeTree()}
}

func (rc *RangeCache) Add(r *Range) {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	rc.rangesMap[r.ID()] = r
	rc.rangesTree.update(r.Range)
}

func (rc *RangeCache) Delete(id uint64) (*Range) {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	if r, find := rc.rangesMap[id]; find {
		delete(rc.rangesMap, id)
		rc.rangesTree.remove(r.Range)
		return r
	}
	return nil
}

func (rc *RangeCache) FindRangeByID(id uint64) (*Range, bool) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	if r, find := rc.rangesMap[id]; find {
		return r, true
	}
	return nil, false
}

func (rc *RangeCache) SearchRange(key []byte) (*Range, bool) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	r := rc.rangesTree.search(key)
	if r == nil {
		return nil, false
	}
	if rng, find := rc.rangesMap[r.GetId()]; find {
		return rng, true
	}
	return nil, false
}

func (rc *RangeCache) MultipleSearchRanges(key []byte, num int) ([]*Range, bool) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	rs := rc.rangesTree.multipleSearch(key, num)
	if len(rs) == 0 {
		return nil, false
	}
	var ranges []*Range
	ranges = make([]*Range, 0, len(rs))
	for _, r := range rs {
		if rng, find := rc.rangesMap[r.GetId()]; find {
			ranges = append(ranges, rng)
		}
	}

	return ranges, true
}

func (rc *RangeCache) GetAllRange() []*Range {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	var ranges []*Range
	for _, r := range rc.rangesMap {
		ranges = append(ranges, r)
	}
	return ranges
}

func (rc *RangeCache) GetTableAllRanges(tableId uint64) []*Range {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	var ranges []*Range
	for _, r := range rc.rangesMap {
		if r.GetTableId() == tableId {
			ranges = append(ranges, r)
		}
	}
	return ranges
}

func (rc *RangeCache) GetTableAllRangesFromTopology(tableId uint64) []*metapb.Range {
	var ranges []*metapb.Range
	var startKeyTopy, endKeyTopy []byte

	rc.lock.RLock()
	defer rc.lock.RUnlock()
	rc.rangesTree.tree.Descend(func(i btree.Item) bool {
		rng := i.(*rangeItem).region
		if rng.GetTableId() != tableId {
			return true
		}
		if startKeyTopy == nil {
			startKeyTopy = rng.StartKey
		}
		endKeyTopy = rng.EndKey
		ranges = append(ranges, rng)
		return true
	})

	return ranges
}

//completeness check
func (rc *RangeCache) GetTableTopologyMissing(tableId uint64) []*metapb.Range {
	var startKey, endKey []byte
	startKey = util.EncodeStorePrefix(util.Store_Prefix_KV, tableId)
	_, endKey = bytesPrefix(startKey)

	var ranges []*metapb.Range
	var startKeyTopy, endKeyTopy []byte

	rc.lock.RLock()
	var rngBefore *metapb.Range
	rc.rangesTree.tree.Descend(func(i btree.Item) bool {
		rng := i.(*rangeItem).region
		if rng.GetTableId() != tableId {
			return true
		}
		if startKeyTopy == nil {
			startKeyTopy = rng.StartKey
		}
		endKeyTopy = rng.EndKey
		if rngBefore != nil {
			if bytes.Compare(rngBefore.EndKey, rng.StartKey) == -1 {
				ranges = append(ranges, &metapb.Range{StartKey: rngBefore.EndKey, EndKey: rng.StartKey})
			} else if bytes.Compare(rngBefore.EndKey, rng.StartKey) == 1 {
				log.Error("must bug!  table [%d] topology scope[%s] and [%s] exists overlap", tableId, rngBefore.EndKey, rng.StartKey)
			}
		}
		rngBefore = rng
		return true
	})
	rc.lock.RUnlock()

	if startKeyTopy == nil && endKeyTopy == nil {
		ranges = append(ranges, &metapb.Range{StartKey: startKey, EndKey: endKey})
	} else {
		if bytes.Compare(startKeyTopy, startKey) == -1 || bytes.Compare(endKeyTopy, endKey) == 1 {
			log.Error("table [%d] scope[%s, %s] is over startKeyLimit[%s], endKeyLimit[%s]", tableId, startKeyTopy, endKeyTopy, startKey, endKey)
		} else {
			if bytes.Compare(startKeyTopy, startKey) == 1 {
				ranges = append(ranges, &metapb.Range{StartKey: startKey, EndKey: startKeyTopy})
			}
			if bytes.Compare(endKeyTopy, endKey) == -1 {
				ranges = append(ranges, &metapb.Range{StartKey: endKeyTopy, EndKey: endKey})
			}
		}
	}
	log.Info("table:[%s, %s] topology tree scope is [%s, %s]", startKey, endKey, startKeyTopy, endKeyTopy)
	return ranges
}

func (rc *RangeCache) GetTableRangeDuplicate(tableId uint64) []*metapb.Range {
	var ranges []*metapb.Range
	rangeDuplMap := make(map[string][]uint64)
	jointFunc := func(startKey, endKey []byte) string {
		return fmt.Sprintf("%s%s", startKey, endKey)
	}
	var rangeIdSlice []uint64
	rc.lock.RLock()
	for _, r := range rc.rangesMap {
		if r.GetTableId() == tableId {
			key := jointFunc(r.StartKey, r.EndKey)
			rangeIdSlice = rangeDuplMap[key]
			rangeIdSlice = append(rangeIdSlice, r.GetId())
			rangeDuplMap[key] = rangeIdSlice
		}
	}
	for _, slice := range rangeDuplMap {
		if len(slice) > 1 {
			for _, rngId := range slice {
				rng := rc.rangesMap[rngId]
				ranges = append(ranges, rng.Range)
			}
		}
	}
	rc.lock.RUnlock()
	return ranges
}

func (rc *RangeCache) GetAllRangeFromTopology() []*metapb.Range {
	var ranges []*metapb.Range
	rc.lock.RLock()
	rc.rangesTree.tree.Descend(func(i btree.Item) bool {
		rng := i.(*rangeItem).region
		ranges = append(ranges, rng)
		return true
	})
	rc.lock.RUnlock()

	return ranges
}

func (rc *RangeCache) GetRandomRange() *Range {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	// 利用map遍历的随机性
	for _, r := range rc.rangesMap {
		return r
	}
	return nil
}

func (rc *RangeCache) Size() int {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	return len(rc.rangesMap)
}

var _ btree.Item = &rangeItem{}

type rangeItem struct {
	region *metapb.Range
}

// Less returns true if the region start key is greater than the other.
// So we will sort the region with start key reversely.
func (r *rangeItem) Less(other btree.Item) bool {
	left := r.region.GetStartKey()
	right := other.(*rangeItem).region.GetStartKey()
	return bytes.Compare(left, right) > 0
}

func (r *rangeItem) Contains(key []byte) bool {
	start, end := r.region.GetStartKey(), r.region.GetEndKey()
	return bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) < 0
}

const (
	defaultBTreeDegree = 64
)

type rangeTree struct {
	tree *btree.BTree
}

func newRangeTree() *rangeTree {
	return &rangeTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

func (t *rangeTree) length() int {
	return t.tree.Len()
}

// update updates the tree with the region.
// It finds and deletes all the overlapped regions first, and then
// insert the region.
func (t *rangeTree) update(rng *metapb.Range) {
	item := &rangeItem{region: rng}

	result := t.find(rng)
	if result == nil {
		result = item
	}

	var overlaps []*rangeItem
	var count int
	t.tree.DescendLessOrEqual(result, func(i btree.Item) bool {
		over := i.(*rangeItem)
		if bytes.Compare(rng.EndKey, over.region.StartKey) <= 0 {
			return false
		}
		overlaps = append(overlaps, over)
		count++
		return true
	})

	if count > 2 {
		log.Warn("=========many overlaps ranges %v, new range[%v]", overlaps, rng)
	}
	for _, item := range overlaps {
		t.tree.Delete(item)
	}

	t.tree.ReplaceOrInsert(item)
}

// remove removes a region if the region is in the tree.
// It will do nothing if it cannot find the region or the found region
// is not the same with the region.
func (t *rangeTree) remove(rng *metapb.Range) {
	result := t.find(rng)
	if result == nil || result.region.GetId() != rng.GetId() {
		return
	}

	t.tree.Delete(result)
}

// search returns a region that contains the key.
func (t *rangeTree) search(key []byte) *metapb.Range {
	rng := &metapb.Range{StartKey: key}
	log.Debug("################### len=%v", t.tree.Len())
	result := t.find(rng)
	if result == nil {
		return nil
	}
	return result.region
}

func (t *rangeTree) multipleSearch(key []byte, num int) []*metapb.Range {
	rng := &metapb.Range{StartKey: key}
	results := t.ascendScan(rng, num)
	var ranges []*metapb.Range
	ranges = make([]*metapb.Range, 0, num)
	var endKey []byte
	for _, r := range results {
		if len(endKey) != 0 {
			if bytes.Compare(r.region.GetStartKey(), endKey) != 0 {
				break
			}
		}
		ranges = append(ranges, r.region)
		endKey = r.region.GetEndKey()
	}
	return ranges
}

// This is a helper function to find an item.
func (t *rangeTree) find(rng *metapb.Range) *rangeItem {
	item := &rangeItem{region: rng}

	var result *rangeItem
	t.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*rangeItem)
		return false
	})

	log.Debug("####range find: result=%v, startkey=%v", result, rng.StartKey)

	if result != nil {
		log.Debug("####range find: result range =%v, startkey=%v", result.region, rng.StartKey)
	}

	if result == nil || !result.Contains(rng.StartKey) {
		return nil
	}

	return result
}

func (t *rangeTree) ascendScan(rng *metapb.Range, num int) []*rangeItem {
	result := t.find(rng)
	if result == nil {
		return nil
	}

	var results []*rangeItem
	//var firstItem *rangeItem
	results = make([]*rangeItem, 0, num)
	count := 0
	t.tree.DescendLessOrEqual(result, func(i btree.Item) bool {
		results = append(results, i.(*rangeItem))
		count++
		if count == num {
			return false
		} else {
			return true
		}
	})
	return results
}

func (t *rangeTree) Min() *rangeItem {
	item := t.tree.Min()
	if item == nil {
		return nil
	}
	return item.(*rangeItem)

}

type RegionCache struct {
	lock      sync.RWMutex
	rangesMap map[uint64]*Range
}

func NewRegionCache() *RegionCache {
	return &RegionCache{rangesMap: make(map[uint64]*Range)}
}

func (rc *RegionCache) Add(r *Range) {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	rc.rangesMap[r.ID()] = r
}

func (rc *RegionCache) Delete(id uint64) {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	if _, find := rc.rangesMap[id]; find {
		delete(rc.rangesMap, id)
	}
}

func (rc *RegionCache) FindRangeByID(id uint64) (*Range, bool) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	if r, find := rc.rangesMap[id]; find {
		return r, true
	}
	return nil, false
}

func (rc *RegionCache) GetAllRange() []*Range {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	var ranges []*Range
	for _, r := range rc.rangesMap {
		ranges = append(ranges, r)
	}
	return ranges
}

func (rc *RegionCache) GetRandomRange() *Range {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	// 利用map遍历的随机性
	for _, r := range rc.rangesMap {
		return r
	}
	return nil
}

func (rc *RegionCache) Size() int {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	return len(rc.rangesMap)
}

type cacheItem struct {
	key    uint64
	value  interface{}
	expire time.Time
}

type idCache struct {
	*expireRegionCache
}

func newIDCache(interval, ttl time.Duration) *idCache {
	return &idCache{
		expireRegionCache: newExpireRegionCache(interval, ttl),
	}
}

func (c *idCache) set(id uint64) {
	c.expireRegionCache.set(id, nil)
}

func (c *idCache) get(id uint64) bool {
	_, ok := c.expireRegionCache.get(id)
	return ok
}

// expireRegionCache is an expired region cache.
type expireRegionCache struct {
	sync.RWMutex

	items      map[uint64]cacheItem
	ttl        time.Duration
	gcInterval time.Duration
}

// newExpireRegionCache returns a new expired region cache.
func newExpireRegionCache(gcInterval time.Duration, ttl time.Duration) *expireRegionCache {
	c := &expireRegionCache{
		items:      make(map[uint64]cacheItem),
		ttl:        ttl,
		gcInterval: gcInterval,
	}

	go c.doGC()
	return c
}

func (c *expireRegionCache) get(key uint64) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()

	item, ok := c.items[key]
	if !ok {
		return nil, false
	}

	if item.expire.Before(time.Now()) {
		return nil, false
	}

	return item.value, true
}

func (c *expireRegionCache) set(key uint64, value interface{}) {
	c.setWithTTL(key, value, c.ttl)
}

func (c *expireRegionCache) setWithTTL(key uint64, value interface{}, ttl time.Duration) {
	c.Lock()
	defer c.Unlock()

	c.items[key] = cacheItem{
		value:  value,
		expire: time.Now().Add(ttl),
	}
}

func (c *expireRegionCache) delete(key uint64) {
	c.Lock()
	defer c.Unlock()

	delete(c.items, key)
}

func (c *expireRegionCache) count() int {
	c.RLock()
	defer c.RUnlock()

	return len(c.items)
}

func (c *expireRegionCache) doGC() {
	ticker := time.NewTicker(c.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			count := 0
			now := time.Now()
			c.Lock()
			for key := range c.items {
				if value, ok := c.items[key]; ok {
					if value.expire.Before(now) {
						count++
						delete(c.items, key)
					}
				}
			}
			c.Unlock()

			log.Debug("GC %d items", count)
		}
	}
}

type lruCache struct {
	sync.RWMutex

	// maxCount is the maximum number of items.
	// 0 means no limit.
	maxCount int

	ll    *list.List
	cache map[uint64]*list.Element
}

// newLRUCache returns a new lru cache.
func newLRUCache(maxCount int) *lruCache {
	return &lruCache{
		maxCount: maxCount,
		ll:       list.New(),
		cache:    make(map[uint64]*list.Element),
	}
}

func (c *lruCache) add(key uint64, value interface{}) {
	c.Lock()
	defer c.Unlock()

	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		ele.Value.(*cacheItem).value = value
		return
	}

	kv := &cacheItem{key: key, value: value}
	ele := c.ll.PushFront(kv)
	c.cache[key] = ele
	if c.maxCount != 0 && c.ll.Len() > c.maxCount {
		c.removeOldest()
	}
}

func (c *lruCache) get(key uint64) (interface{}, bool) {
	c.Lock()
	defer c.Unlock()

	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		return ele.Value.(*cacheItem).value, true
	}

	return nil, false
}

func (c *lruCache) peek(key uint64) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()

	if ele, ok := c.cache[key]; ok {
		return ele.Value.(*cacheItem).value, true
	}

	return nil, false
}

func (c *lruCache) remove(key uint64) {
	c.Lock()
	defer c.Unlock()

	if ele, ok := c.cache[key]; ok {
		c.removeElement(ele)
	}
}

func (c *lruCache) removeOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

func (c *lruCache) removeElement(ele *list.Element) {
	c.ll.Remove(ele)
	kv := ele.Value.(*cacheItem)
	delete(c.cache, kv.key)
}

func (c *lruCache) elems() []*cacheItem {
	c.RLock()
	defer c.RUnlock()

	elems := make([]*cacheItem, 0, c.ll.Len())
	for ele := c.ll.Front(); ele != nil; ele = ele.Next() {
		clone := *(ele.Value.(*cacheItem))
		elems = append(elems, &clone)
	}

	return elems
}

func (c *lruCache) len() int {
	c.RLock()
	defer c.RUnlock()

	return c.ll.Len()
}

type fifoCache struct {
	sync.RWMutex

	// maxCount is the maximum number of items.
	// 0 means no limit.
	maxCount int

	ll *list.List
}

// newFifoCache returns a new fifo cache.
func newFifoCache(maxCount int) *fifoCache {
	return &fifoCache{
		maxCount: maxCount,
		ll:       list.New(),
	}
}

func (c *fifoCache) add(key uint64, value interface{}) {
	c.Lock()
	defer c.Unlock()

	kv := &cacheItem{key: key, value: value}
	c.ll.PushFront(kv)

	if c.maxCount != 0 && c.ll.Len() > c.maxCount {
		c.ll.Remove(c.ll.Back())
	}
}

func (c *fifoCache) remove() {
	c.Lock()
	defer c.Unlock()

	c.ll.Remove(c.ll.Back())
}

func (c *fifoCache) elems() []*cacheItem {
	c.RLock()
	defer c.RUnlock()

	elems := make([]*cacheItem, 0, c.ll.Len())
	for ele := c.ll.Back(); ele != nil; ele = ele.Prev() {
		elems = append(elems, ele.Value.(*cacheItem))
	}

	return elems
}

func (c *fifoCache) fromElems(key uint64) []*cacheItem {
	c.RLock()
	defer c.RUnlock()

	elems := make([]*cacheItem, 0, c.ll.Len())
	for ele := c.ll.Back(); ele != nil; ele = ele.Prev() {
		kv := ele.Value.(*cacheItem)
		if kv.key > key {
			elems = append(elems, ele.Value.(*cacheItem))
		}
	}

	return elems
}

func (c *fifoCache) len() int {
	c.RLock()
	defer c.RUnlock()

	return c.ll.Len()
}

type GlobalDeletedRange struct {
	lock          sync.RWMutex
	deletedRanges map[uint64]*metapb.Range
}

func NewGlobalDeletedRange() *GlobalDeletedRange {
	return &GlobalDeletedRange{
		deletedRanges: make(map[uint64]*metapb.Range)}
}

func (rc *GlobalDeletedRange) Add(rng *metapb.Range) {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	rc.deletedRanges[rng.GetId()] = rng
}

func (rc *GlobalDeletedRange) Delete(id uint64) {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	if _, find := rc.deletedRanges[id]; find {
		delete(rc.deletedRanges, id)
	}
}

func (rc *GlobalDeletedRange) FindRange(id uint64) (*metapb.Range, bool) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	if rng, find := rc.deletedRanges[id]; find {
		return rng, true
	}
	return nil, false
}

type GlobalPreGCRange struct {
	lock        sync.RWMutex
	preGCRanges map[uint64]*metapb.Range
}

func NewGlobalPreGCRange() *GlobalPreGCRange {
	return &GlobalPreGCRange{
		preGCRanges: make(map[uint64]*metapb.Range)}
}

func (rc *GlobalPreGCRange) Add(rng *metapb.Range) {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	rc.preGCRanges[rng.GetId()] = rng
}

func (rc *GlobalPreGCRange) Delete(id uint64) {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	if _, find := rc.preGCRanges[id]; find {
		delete(rc.preGCRanges, id)
	}
}

func (rc *GlobalPreGCRange) FindRange(id uint64) (*metapb.Range, bool) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	if rng, find := rc.preGCRanges[id]; find {
		return rng, true
	}
	return nil, false
}
