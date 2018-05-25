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
package mock_ms

import (
	"bytes"
	"sync"

	"model/pkg/metapb"
	"util/log"

	"github.com/google/btree"
)

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

func (rc *RangeCache) Delete(id uint64) {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	if r, find := rc.rangesMap[id]; find {
		delete(rc.rangesMap, id)
		rc.rangesTree.remove(r.Range)
	}
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
