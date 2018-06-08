package server

import (
	"sync"
)

// TaskManager task manager
type TaskManager struct {
	sync.RWMutex
	tasks map[uint64]*TaskChain // key: range id
}

// NewTaskManager create task manager
func NewTaskManager() *TaskManager {
	return &TaskManager{
		tasks: make(map[uint64]*TaskChain),
	}
}

// Add add a taskchain
func (m *TaskManager) Add(tc *TaskChain) bool {
	m.Lock()
	defer m.Unlock()

	if old, ok := m.tasks[tc.GetRangeID()]; ok {
		_ = old
		return false
	}
	m.tasks[tc.GetRangeID()] = tc

	return true
}

// Remove remove
func (m *TaskManager) Remove(rangeID uint64) {
	m.Lock()
	defer m.Unlock()

	delete(m.tasks, rangeID)
}

// Find find
func (m *TaskManager) Find(rangeID uint64) *TaskChain {
	m.RLock()
	defer m.RUnlock()

	return m.tasks[rangeID]
}
