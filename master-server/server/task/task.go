package task

import (
	"fmt"
	"model/pkg/taskpb"
	"sync/atomic"
	"time"
)

// Task range task interface
type Task interface {
	// GetID return task id
	GetID() uint64

	// GetType return task type
	GetType() TaskType

	// GetRangeID return the range's id which the task belong to
	GetRangeID() uint64

	// Step next step
	Step(cluster *Cluster, r *Range) (over bool, task *taskpb.Task, err error)

	// GetState return current state
	GetState() TaskStateType

	// IsOver return true if task is over
	IsOver() bool

	// IsTimeout return true if task is run timeout
	IsTimeout() bool

	// Elapsed time elapsed since task start
	Elapsed() time.Duration

	// String to string for print
	String() string
}

// BaseTask include task's common attrs
type BaseTask struct {
	id       uint64
	rangeID  uint64
	typ      TaskType
	state    TaskStateType
	begin    time.Time
	update   time.Time
	timeout  time.Duration
	stepping uint64
}

func NewBaseTask(id uint64, rangeID uint64, typ TaskType, timeout time.Duration) *BaseTask {
	return &BaseTask{
		id:       id,
		rangeID:  rangeID,
		typ:      typ,
		state:    TaskStateStart,
		begin:    time.Now(),
		update:   time.Now(),
		timeout:  timeout,
		stepping: 0,
	}
}

// GetID return id
func (t *BaseTask) GetID() uint64 {
	return t.id
}

// GetType return task type
func (t *BaseTask) GetType() TaskType {
	return t.typ
}

// GetRangeID return range id
func (t *BaseTask) GetRangeID() uint64 {
	return t.rangeID
}

// GetState return current state
func (t *BaseTask) GetState() TaskStateType {
	return t.state
}

// IsTimeout return true if task is run timeout
func (t *BaseTask) IsTimeout() bool {
	return t.state == TaskStateTimeout || time.Since(t.begin) > t.timeout
}

// Elapsed time elapsed since task start
func (t *BaseTask) Elapsed() time.Duration {
	return time.Since(t.begin)
}

func (t *BaseTask) String() string {
	return fmt.Sprintf("\"id\": %d, \"range_id\": %d, \"type\": \"%s\", "+
		"\"state\": \"%s\", \"begin\": \"%s\", \"update\": \"%s\"",
		t.id, t.rangeID, t.typ.String(), t.state.String(), t.begin.String(), t.update.String())
}

func (t *BaseTask) markAsStepping() bool {
	return atomic.CompareAndSwapUint64(&t.stepping, 0, 1)
}

func (t *BaseTask) unmarkStepping() {
	atomic.StoreUint64(&t.stepping, 0)
}
