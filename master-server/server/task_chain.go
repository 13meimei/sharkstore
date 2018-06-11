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
	loggingID  string
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
		loggingID:  fmt.Sprintf("run %s tasks for range(%d)", name, rangeID),
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
func (c *TaskChain) Next(cluster *Cluster, r *Range) (over bool, task *taskpb.Task) {
	// only one goroutine can execute it at the same time
	if !atomic.CompareAndSwapUint64(&c.running, 0, 1) {
		log.Warn("%s failed: already running", c.loggingID)
		return false, nil
	}
	defer atomic.StoreUint64(&c.running, 0)

	c.lastUpdate = time.Now()

	for {
		if c.curIdx >= len(c.tasks) {
			return true, nil
		}

		t := c.tasks[c.curIdx]
		over = t.CheckOver()
		if !over {
			over, task = t.Step(cluster, r)
		}

		// not over, return and wait next Next
		if !over {
			return false, task
		}

		// current task is over but it doesn't finished successfully. so failed at this point
		if t.GetState() != TaskStateFinished {
			log.Error("%s failed. last %s task failed at %s, detail: %s",
				c.loggingID, t.GetType().String(), t.GetState().String(), t.String())
			return true, nil
		}

		// current task finished successfully and current is the last one
		if c.curIdx == len(c.tasks)-1 {
			log.Info("%s finished. last task: %s", c.loggingID, t)
			return true, nil
		}

		// current task finished successfully but there is other task leftover, switch to run next task
		c.curIdx++
		// reset next task's begin time
		c.tasks[c.curIdx].SetBeginTime()
		log.Info("%s start next task: %s", c.loggingID, c.tasks[c.curIdx].String())
	}
}

func (c *TaskChain) String() string {
	return fmt.Sprintf("{\"name\": \"%s\", \"range\": %d, \"begin\": %v, \"update\": %v, \"tasks\": %v}",
		c.name, c.rangeID, c.begin, c.lastUpdate, c.tasks)
}
