package server

import (
	"testing"
	"time"
	"model/pkg/metapb"
)

func TestMetricTask(t *testing.T) {
	metric := NewMetric(nil, "192.168.108.111:8887", time.Second)
	metric.Run()
	event := NewAddPeerEvent(12, 1, &metapb.Peer{Id: 2, NodeId: 1}, "test")
	metric.CollectEvent(event)
	event = NewAddPeerEvent(22, 1, &metapb.Peer{Id: 3, NodeId: 1}, "test2")
	metric.CollectEvent(event)
	complexEvent := NewChangePeerEvent(32, &Range{Range: &metapb.Range{Id: 1}}, &metapb.Peer{Id: 3, NodeId: 1}, &metapb.Peer{Id: 4, NodeId: 2},
		"test3")
	if len(metric.eventList) != 2 {
		t.Fatal("test failed")
		return
	}
	if metric.eventList[0].GetRangeID() != 1 {
		t.Fatal("test failed")
		return
	}
	if metric.eventList[1].GetRangeID() != 1 {
		t.Fatal("test failed")
		return
	}
	metric.CollectEvent(complexEvent)
	t.Logf("%v", metric.eventList)
	wg.Add(1)
	wg.Wait()
}

func TestMetricScheduler(t *testing.T) {
	metric := NewMetric(nil, "127.0.0.1:8887", time.Second)
	metric.CollectScheduleCounter("a", "AA")
	metric.CollectScheduleCounter("a", "AA")
	metric.CollectScheduleCounter("b", "BB")
	if metric.scheduleCounter["a"]["AA"] != 2 {
		t.Fatal("test failed")
		return
	}
	if metric.scheduleCounter["b"]["BB"] !=1 {
		t.Fatal("test failed")
		return
	}
}
