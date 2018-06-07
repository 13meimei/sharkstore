package server

import (
	"testing"
	"util/assert"
)

func TestCluster_DeleteRange(t *testing.T) {
	mockCluster := MockCluster(t)
	defer closeCluster(mockCluster)
	addRangeForLeaderBalance(mockCluster, 1, t)
	assert.Equal(t, len(mockCluster.GetAllRanges()), 1, "add range success")
	rng := mockCluster.GetAllRanges()[0]

	id, err := mockCluster.GenId()
	if err != nil {
		return
	}
	event :=  NewDelRangeEvent(id, rng.GetId(), "hb range remove")
	needNext, dispatchTask, err := event.Execute(mockCluster, rng)
	if needNext && dispatchTask == nil && err == nil {
		t.Logf("event execute success")
	}
	assert.Equal(t, event.GetStatus(), EVENT_STATUS_FINISH, "event success")
}

func TestCluster_DeleteRangeByHb(t *testing.T) {
	mockCluster := MockCluster(t)
	defer closeCluster(mockCluster)
	addRangeForLeaderBalance(mockCluster, 1, t)
	assert.Equal(t, len(mockCluster.GetAllRanges()), 1, "add range success")
	rng := mockCluster.GetAllRanges()[0]

	id, err := mockCluster.GenId()
	if err != nil {
		return
	}
	event :=  NewDelRangeEvent(id, rng.GetId(), "hb range remove")

	mockCluster.eventDispatcher.pushEvent(event)
	//mock hb
	task := mockCluster.eventDispatcher.Dispatch(rng)
	if task == nil {
		t.Logf("task success")
	}
	assert.Equal(t, event.GetStatus(), EVENT_STATUS_FINISH, "event success")
	assert.Equal(t, len(mockCluster.GetAllEvent()), 0, "event execute success and remove event")
	assert.Equal(t, len(mockCluster.GetAllRanges()), 0, "range delete success")
}
