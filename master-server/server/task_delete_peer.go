package server

import (
	"fmt"
	"time"
	"util/log"

	"model/pkg/metapb"
	"model/pkg/taskpb"
)

const (
	defaultDelPeerTaskTimeout = time.Second * time.Duration(30)
)

// DeletePeerTask  delete peer task
type DeletePeerTask struct {
	*BaseTask
	peer *metapb.Peer // peer to delete

	confRetries   int
	deleteRetries int
}

// NewDeletePeerTask new delete peer task
func NewDeletePeerTask(peer *metapb.Peer) *DeletePeerTask {
	return &DeletePeerTask{
		BaseTask: newBaseTask(TaskTypeDeletePeer, defaultDelPeerTaskTimeout),
		peer:     peer,
	}
}

func (t *DeletePeerTask) String() string {
	return fmt.Sprintf("{%s, \"to_delete\":\"%s\"}", t.BaseTask.String(), t.peer.String())
}

// Step step
func (t *DeletePeerTask) Step(cluster *Cluster, r *Range) (over bool, task *taskpb.Task) {
	switch t.GetState() {
	case TaskStateStart:
		return false, t.stepStart(r)
	case WaitRaftConfReady:
		return t.stepWaitConf(cluster, r)
	case WaitRangeDeleted:
		return t.stepDeleteRange(cluster, r), nil
	default:
		log.Error("%s unexpceted add peer task state: %s", t.logID, t.state.String())
	}
	return
}

func (t *DeletePeerTask) issueTask() *taskpb.Task {
	return &taskpb.Task{
		Type: taskpb.TaskType_RangeDelPeer,
		RangeDelPeer: &taskpb.TaskRangeDelPeer{
			Peer: t.peer,
		},
	}
}

func (t *DeletePeerTask) stepStart(r *Range) *taskpb.Task {
	t.state = WaitRaftConfReady
	return t.issueTask()
}

func (t *DeletePeerTask) stepWaitConf(cluster *Cluster, r *Range) (over bool, task *taskpb.Task) {
	if r.GetPeer(t.peer.GetId()) != nil {
		t.confRetries++
		return false, t.issueTask()
	}

	log.Info("%s delete raft member finished, peer: %v", t.logID, t.peer)

	over = t.stepDeleteRange(cluster, r)
	return
}

func (t *DeletePeerTask) stepDeleteRange(cluster *Cluster, r *Range) (over bool) {
	node := cluster.FindNodeById(t.peer.GetNodeId())
	if node == nil {
		log.Warn("%s target node(%d) doesn't exist", t.logID, t.peer.GetNodeId())
		t.state = TaskStateCanceled
		return true
	}

	err := cluster.cli.DeleteRange(node.GetServerAddr(), r.GetId())
	if err == nil {
		log.Error("%s delete range failed, target node: %d, retries: %d", t.logID, t.peer.GetNodeId(), t.deleteRetries)
		t.deleteRetries++
		return false
	}

	log.Info("%s delete range finished, peer: %v", t.logID, t.peer)

	t.state = TaskStateFinished
	return true
}
