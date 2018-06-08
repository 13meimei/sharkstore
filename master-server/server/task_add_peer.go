package server

import (
	"fmt"

	"model/pkg/metapb"
	"model/pkg/taskpb"
	"util/log"
)

// AddPeerTask add peer task
type AddPeerTask struct {
	*BaseTask
	peer *metapb.Peer

	confRetries   int
	createRetries int
}

// NewAddPeerTask new add peer task
func NewAddPeerTask(id uint64, rangeID uint64) *AddPeerTask {
	return &AddPeerTask{
		BaseTask: newBaseTask(id, rangeID, TaskTypeAddPeer, DefaultAddPeerTimeout),
	}
}

func (t *AddPeerTask) String() string {
	return fmt.Sprintf("{%s, \"to_add\":\"%s\"}", t.BaseTask.String(), t.peer.String())
}

// Step step
func (t *AddPeerTask) Step(cluster *Cluster, r *Range) (over bool, task *taskpb.Task, err error) {
	if !t.markAsStepping() {
		return
	}
	defer t.unmarkStepping()

	// task is over
	if t.CheckOver() {
		return true, nil, nil
	}

	if r == nil {
		log.Warn("% invalid input range: <nil>", t.loggingID)
		return false, nil, nil
	}

	switch t.GetState() {
	case TaskStateStart:
		over = false
		task, err = t.stepStart(cluster, r)
		return
	case WaitRaftConfChanged:
		over = false
		task, err = t.stepWaitConf(cluster, r)
		return
	case WaitRangeCreated:
		over = false
		err = t.stepCreateRange(cluster, r)
		return
	case WaitDataSynced:
		over = t.stepWaitSync(r)
		return
	default:
		err = fmt.Errorf("unexpceted add peer task state: %s", t.state.String())
		over = true
	}
	return
}

func (t *AddPeerTask) issueTask() *taskpb.Task {
	return &taskpb.Task{
		Type: taskpb.TaskType_RangeAddPeer,
		RangeAddPeer: &taskpb.TaskRangeAddPeer{
			Peer: t.peer,
		},
	}
}

func (t *AddPeerTask) stepStart(cluster *Cluster, r *Range) (task *taskpb.Task, err error) {
	// not alloc new peer yet
	if t.peer == nil {
		t.peer, err = cluster.allocPeerAndSelectNode(r)
		if err != nil {
			log.Error("%s alloc peer failed: %s", t.loggingID, err.Error())
			return nil, err
		}
	}

	t.state = WaitRaftConfChanged

	// return a task to add this peer into raft member
	return t.issueTask(), nil
}

func (t *AddPeerTask) stepWaitConf(cluster *Cluster, r *Range) (task *taskpb.Task, err error) {
	if r.GetPeer(t.peer.GetId()) == nil {
		t.confRetries++

		return t.issueTask(), nil
	}

	t.state = WaitRangeCreated

	err = t.stepCreateRange(cluster, r)
	return nil, err
}

func (t *AddPeerTask) stepCreateRange(cluster *Cluster, r *Range) error {
	err := prepareAddPeer(cluster, r, t.peer)
	if err != nil {
		log.Error("%s create new range failed: %s, retries: %d", t.loggingID, err.Error(), t.createRetries)
		t.createRetries++
		return err
	}

	t.state = WaitDataSynced
	return nil
}

func (t *AddPeerTask) stepWaitSync(r *Range) bool {
	if r.GetPendingPeer(t.peer.GetId()) != nil {
		return false
	}

	peer := r.GetPeer(t.peer.GetId())
	if peer == nil {
		log.Error("%s could not find target peer(%v) when check data sync", t.loggingID, t.peer)
		return false
	}
	if peer.Type == metapb.PeerType_PeerType_Learner {
		return false
	}

	t.state = TaskStateFinished
	return true
}
