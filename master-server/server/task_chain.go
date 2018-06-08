package server

import (
	"fmt"
	"sync/atomic"
	"time"

	"model/pkg/taskpb"
	"util/log"
)

// TaskChain is a list of associated tasks to achive the same goal
type TaskChain struct {
	rangeID    uint64
	name       string
	tasks      []Task // all tasks
	curIdx     int    // current running task idx
	begin      time.Time
	lastUpdate time.Time
	running    uint64
}

// NewTaskChain new taskchain
func NewTaskChain(rangeID uint64, name string, tasks []Task) *TaskChain {
	c := &TaskChain{
		rangeID:    rangeID,
		name:       name,
		tasks:      tasks,
		curIdx:     0,
		begin:      time.Now(),
		lastUpdate: time.Now(),
		running:    0,
	}
	return c
}

// GetRangeID return range id
func (c *TaskChain) GetRangeID() uint64 {
	return c.rangeID
}

// Elapsed time since begin
func (c *TaskChain) Elapsed() time.Duration {
	return time.Since(c.begin)
}

// Next run next step
func (c *TaskChain) Next(cluster *Cluster, r *Range) (over bool, task *taskpb.Task, err error) {
	// only one goroutine can execute it at the same time
	if !atomic.CompareAndSwapUint64(&c.running, 0, 1) {
		return false, nil, fmt.Errorf("already running")
	}
	defer atomic.StoreUint64(&c.running, 0)

	c.lastUpdate = time.Now()

	for {
		if c.curIdx >= len(c.tasks) {
			return true, nil, nil
		}

		t := c.tasks[c.curIdx]
		over, task, err = t.Step(cluster, r)
		if err != nil {
			log.Error("run %s taskchain for range(%d) error at the %d task(%s): %v, will retry later",
				c.name, c.rangeID, c.curIdx, t.String(), err)
			// eat this error and wait next time retry
			return false, nil, nil
		}

		// not over, return and wait next Next
		if !over {
			return false, task, nil
		}

		// current task is over but it doesn't finished successfully. so failed at this point
		if t.GetState() != TaskStateFinished {
			log.Error("run %s taskchain for range(%d) failed. last task: %s", c.name, c.rangeID, t.String())
			return true, nil, fmt.Errorf("do task: %v failed. ", t.String())
		}

		// current task finished successfully and current is the last one
		if c.curIdx == len(c.tasks)-1 {
			log.Info("run %s taskchain for range(%d) over. last task: %s", c.name, c.rangeID, t)
			return true, nil, nil
		}

		// current task finished successfully and there is other task leftover, switch run next task
		c.curIdx++
		// reset next task's begin time
		c.tasks[c.curIdx].SetBeginTime()
		log.Info("run %s taskchain for range(%d) start next task: %s", c.name, c.rangeID, c.tasks[c.curIdx].String())
	}
}

func (c *TaskChain) String() string {
	return fmt.Sprintf("{\"name\": \"%s\", \"range\": %d, \"begin\": %v, \"update\": %v, \"tasks\": %v}",
		c.name, c.rangeID, c.begin, c.lastUpdate, c.tasks)
}
