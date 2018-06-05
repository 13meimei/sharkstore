// TaskType the task type
type TaskType int

const (
	// TaskTypeAddPeer add peer type
	TaskTypeAddPeer TaskType = iota + 1
	// TaskTypeDeletePeer delete peer
	TaskTypeDeletePeer
	// TaskTypeChangeLeader change leader
	TaskTypeChangeLeader
	// TaskTypeDeleteRange delete range
	TaskTypeDeleteRange
)

// String task type to string name
func (t TaskType) String() string {
	switch t {
	case TaskTypeAddPeer:
		return "add peer"
	case TaskTypeDeletePeer:
		return "delete peer"
	case TaskTypeChangeLeader:
		return "change leader"
	case TaskTypeDeleteRange:
		return "delete range"
	default:
		return "unknown"
	}
}

// TaskStateType task running state
type TaskStateType int

const (
	// TaskStateStart start
	TaskStateStart TaskStateType = iota + 1
	// TaskStateFinished finished
	TaskStateFinished
	// TaskStateFailed  failed
	TaskStateFailed
	// TaskStateCanceled canceled
	TaskStateCanceled
	// TaskStateTimeout run timeout
	TaskStateTimeout

	// TaskStateConfReady raft conf ready
	TaskStateConfReady
)

// String to string name
// TODO: use a table-driven pattern
func (ts TaskStateType) String() string {
	switch ts {
	case TaskStateStart:
		return "start"
	case TaskStateFinished:
		return "finished"
	case TaskStateFailed:
		return "failed"
	case TaskStateCanceled:
		return "canceled"
	case TaskStateTimeout:
		return "timeout"
	case TaskStateConfReady:
		return "conf ready"
	default:
		return "unknown"
	}
}