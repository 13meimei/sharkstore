package task

import "model/pkg/taskpb"

type AddPeerTask struct {
	BaseTask
}

func NewAddPeerTask() {
	// TODO:
	return &AddPeerTask{}
}

func (t *AddPeerTask) String() string {
}

func (t *AddPeerTask) Step(cluster *Cluster, r *Range) (over bool, task *taskpb.Task, err error) {
	if !t.markAsStepping() {
		return
	}
	defer t.unmarkStepping()
}
