package server

import (
	"testing"
	"fmt"
	"model/pkg/metapb"
	"github.com/stretchr/testify/assert"
)

/**
	RANGE<10> CHANGE RANGE EVENT
	Peers：
		rangeID nodeId leader
		11		2		是
		12		3		否
		13		4		否

	Peers After Changed:
		rangeID nodeId leader
		14		5		是
		12		3		否
		13		4		否
 */
func TestDispatch(t *testing.T) {
	mockCluster := MockCluster(t)
	defer closeCluster(mockCluster)
	assert := assert.New(t)
	/*
		rangeID nodeId leader
		11		2		是
		12		3		否
		13		4		否
	 */
	rngM := &metapb.Range{
		Id: 10,
		Peers: []*metapb.Peer{{Id: 11, NodeId: 2}, {Id: 12, NodeId: 3}, {Id: 13, NodeId: 4}},
	}
	rng := NewRange(rngM, nil)
	mockCluster.AddRange(rng)
	assert.Equal(len(mockCluster.GetAllRanges()), 1, "add range success")
	/**
		change to
		rangeID nodeId leader
		14		5		是
		12		3		否
		13		4		否
	 */

	/** -------first change event-----------*/
	ok := addChangeEvent(mockCluster, rng)
	assert.Equal(ok, true, "add change event for range 10 success.")
	ok = addChangeEvent(mockCluster, rng)
	assert.Equal(ok, false, "The event of range 10 had existed.")

	event := mockCluster.eventDispatcher.peekEvent(rng.GetId())
	t.Log(fmt.Sprintf("event detail: %v", event))

	//mock first range hb
	task := mockCluster.eventDispatcher.Dispatch(rng)
	if task == nil {
		t.Errorf("exectue add peer task logic error, %v", task)
		return
	}
	t.Log("hb ")
	event = mockCluster.eventDispatcher.peekEvent(rng.GetId())
	if event.GetStatus() != EVENT_STATUS_DEALING {
		t.Errorf("exectue add peer task status error")
		return
	}
	//mock second range hb
	task = mockCluster.eventDispatcher.Dispatch(rng)
	if task == nil {
		t.Errorf("exectue add peer task logic error")
		return
	}
	event = mockCluster.eventDispatcher.peekEvent(rng.GetId())
	if event != nil {
		t.Errorf("dispach task by hb error")
		return
	}

	/** -------second change event-----------*/


}

func addChangeEvent(cluster *Cluster, rng *Range)  bool {
	taskId, _ := cluster.idGener.GenID()
	complexEvent := NewChangePeerEvent(taskId, rng, rng.GetPeer(11), &metapb.Peer{Id: 14, NodeId: 5}, "node-range-balance")
	return cluster.eventDispatcher.pushEvent(complexEvent)
}