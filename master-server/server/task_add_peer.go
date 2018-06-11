package server

import (
	"fmt"
	"time"

	"model/pkg/metapb"
	"model/pkg/taskpb"
	"util/log"
)

const (
	defaultAddPeerTaskTimeout = time.Second * time.Duration(300)
)

// AddPeerTask add peer task
type AddPeerTask struct {
	*BaseTask
	peer *metapb.Peer // peer to add

	confRetries   int
	createRetries int
}

// NewAddPeerTask new add peer task
func NewAddPeerTask(id uint64, rangeID uint64) *AddPeerTask {
	return &AddPeerTask{
		BaseTask: newBaseTask(id, rangeID, TaskTypeAddPeer, defaultAddPeerTaskTimeout),
	}
}

func (t *AddPeerTask) String() string {
	return fmt.Sprintf("{%s, \"to_add\":\"%s\"}", t.BaseTask.String(), t.peer.String())
}

// Step step
func (t *AddPeerTask) Step(cluster *Cluster, r *Range) (over bool, task *taskpb.Task) {
	switch t.GetState() {
	case TaskStateStart:
		task = t.stepStart(cluster, r)
		return false, task
	case WaitRaftConfReady:
		task = t.stepWaitConf(cluster, r)
		return false, task
	case WaitRangeCreated:
		t.stepCreateRange(cluster, r)
		return false, nil
	case WaitDataSynced:
		return t.stepWaitSync(r), nil
	default:
		log.Error("%s unexpceted add peer task state: %s", t.loggingID, t.state.String())
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

func (t *AddPeerTask) stepStart(cluster *Cluster, r *Range) (task *taskpb.Task) {
	// not alloc new peer yet
	if t.peer == nil {
		var err error
		t.peer, err = cluster.allocPeerAndSelectNode(r)
		if err != nil {
			log.Error("%s alloc peer failed: %s", t.loggingID, err.Error())
			return nil
		}
	}

	t.state = WaitRaftConfReady

	// return a task to add this peer into raft member
	return t.issueTask()
}

func (t *AddPeerTask) stepWaitConf(cluster *Cluster, r *Range) (task *taskpb.Task) {
	if r.GetPeer(t.peer.GetId()) == nil {
		t.confRetries++

		return t.issueTask()
	}

	log.Info("%s add raft member finsihed, peer: %v.", t.loggingID, t.peer)

	t.state = WaitRangeCreated

	t.stepCreateRange(cluster, r)
	return nil
}

func (t *AddPeerTask) stepCreateRange(cluster *Cluster, r *Range) {
	err := prepareAddPeer(cluster, r, t.peer)
	if err != nil {
		log.Error("%s create new range failed: %s, peer: %v, retries: %d", t.loggingID, err.Error(), t.peer, t.createRetries)
		t.createRetries++
		return
	}

	log.Info("%s create range finshed to node(%d)", t.loggingID, t.peer.GetNodeId())

	t.state = WaitDataSynced
	return
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

	log.Info("%s data sync finished, peer: %v", t.loggingID, t.peer)

	t.state = TaskStateFinished
	return true
}
