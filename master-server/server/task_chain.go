package server

import (
	"container/list"
	"fmt"
	"sync/atomic"
	"time"

	"model/pkg/taskpb"
)

// TaskChain is a list of associated tasks to achive the same goal
type TaskChain struct {
	rangeID  uint64
	name     string
	taskList *list.List

	lastUpdate time.Time
	running    uint64
}

// NewTaskChain new taskchain
func NewTaskChain(rangeID uint64, name string, tasks []Task) *TaskChain {
	tc := &TaskChain{
		rangeID:  rangeID,
		name:     name,
		taskList: list.New(),
	}
	for _, task := range tasks {
		tc.taskList.PushBack(task)
	}
	return tc
}

// GetRangeID return range id
func (tc *TaskChain) GetRangeID() uint64 {
	return tc.rangeID
}

// Next run next step
func (tc *TaskChain) Next(culster *Cluster, r *Range) (over bool, task *taskpb.Task, err error) {
	// only one goroutine can execute at the same time
	if !atomic.CompareAndSwapUint64(&tc.running, 0, 1) {
		return false, nil, fmt.Errorf("already running")
	}
	defer atomic.StoreUint64(&tc.running, 0)

	tc.lastUpdate = time.Now()

	// TODO:
	return
}

func (tc *TaskChain) String() string {
	// TODO:
	return ""
}
