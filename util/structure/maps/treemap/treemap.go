// Implements a map backed by llrb tree.
// Elements are ordered by key in the map.
package treemap

import (
	"sync"

	"util/compare"
	"util/structure/trees/llrbtree"
)

// Map holds the elements in a llrb tree
type Map struct {
	isSafe bool
	tree   *llrbtree.Tree
	rwMu   sync.RWMutex
}

func New(isSafe bool) *Map {
	return &Map{isSafe: isSafe, tree: llrbtree.New(llrbtree.LLRB23)}
}

// Put inserts key-value pair into the map.
// Key should adhere to the comparator's type assertion, otherwise method panics.
func (m *Map) Put(key compare.Comparable, value interface{}) {
	if m.isSafe {
		m.rwMu.Lock()
		defer m.rwMu.Unlock()
	}
	m.tree.Insert(key, value)
}

// Get searches the element in the map by key and returns its value.
// Second return parameter is true if key was found, otherwise false.
// Key should adhere to the comparator's type assertion, otherwise method panics.
func (m *Map) Get(key compare.Comparable) (value interface{}, found bool) {
	if m.isSafe {
		m.rwMu.RLock()
		defer m.rwMu.RUnlock()
	}
	k, v := m.tree.Get(key)
	return v, k != nil
}

// Remove the element from the map by key.
// Key should adhere to the comparator's type assertion, otherwise method panics.
func (m *Map) Remove(key compare.Comparable) {
	if m.isSafe {
		m.rwMu.Lock()
		defer m.rwMu.Unlock()
	}
	m.tree.Delete(key)
}

// Empty returns true if map does not contain any elements
func (m *Map) Empty() bool {
	if m.isSafe {
		m.rwMu.RLock()
		defer m.rwMu.RUnlock()
	}
	return m.tree.Empty()
}

// Size returns number of elements in the map.
func (m *Map) Size() int {
	if m.isSafe {
		m.rwMu.RLock()
		defer m.rwMu.RUnlock()
	}
	return m.tree.Len()
}

// Keys returns all keys in-order
func (m *Map) Keys() []compare.Comparable {
	if m.isSafe {
		m.rwMu.RLock()
		defer m.rwMu.RUnlock()
	}
	return m.tree.Keys()
}

// Values returns all values in-order based on the key.
func (m *Map) Values() []interface{} {
	if m.isSafe {
		m.rwMu.RLock()
		defer m.rwMu.RUnlock()
	}
	return m.tree.Values()
}

// Clear removes all elements from the map.
func (m *Map) Clear() {
	if m.isSafe {
		m.rwMu.Lock()
		defer m.rwMu.Unlock()
	}
	m.tree.Clear()
}

// Min returns the minimum key and its value from the tree map.
// Returns nil, nil if map is empty.
func (m *Map) Min() (key compare.Comparable, value interface{}) {
	if m.isSafe {
		m.rwMu.RLock()
		defer m.rwMu.RUnlock()
	}
	return m.Min()
}

// Max returns the maximum key and its value from the tree map.
// Returns nil, nil if map is empty.
func (m *Map) Max() (key compare.Comparable, value interface{}) {
	if m.isSafe {
		m.rwMu.RLock()
		defer m.rwMu.RUnlock()
	}
	return m.Max()
}
