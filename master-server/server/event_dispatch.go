package server

import (
	"model/pkg/taskpb"
	"util/log"
	"sync"
	"golang.org/x/net/context"
)

var (
	hotRegionLowThreshold = 3
)

type EventDispatcher struct {
	ctx            context.Context
	cancel         context.CancelFunc
	lock           sync.RWMutex
	wg             sync.WaitGroup
	cluster        *Cluster
	opt            *scheduleOption
	current_events map[uint64]RangeEvent
}

func NewEventDispatcher(cluster *Cluster, opt *scheduleOption) *EventDispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &EventDispatcher{
		ctx:     ctx,
		cancel:  cancel,
		cluster: cluster,
		opt:     opt,
		//schedulers: make(map[string]*scheduleController),
		current_events: make(map[uint64]RangeEvent),
	}
}
func (dispatcher *EventDispatcher) Dispatch(r *Range) *taskpb.Task {
	DISPATCH_TASK:
	// dispatch task from existed event.
	if e := dispatcher.peekEvent(r.GetId()); e != nil {
		cur := e
		var needNext ExecNextEvent = true
		var dispatchTask *taskpb.Task
		var err error

		for needNext && cur != nil {
			needNext, dispatchTask, err = cur.Execute(dispatcher.cluster, r)
			if !needNext {
				if err != nil {
					return nil
				}
				if err == nil && dispatchTask != nil {
					return dispatchTask
				}
				break
			}
			cur = cur.Next()
		}
		dispatcher.removeEvent(e)
	}

	//生成任务时，要满足条件
	if e := dispatcher.cluster.hbManager.CheckRange(dispatcher.cluster, r); e != nil {
		if dispatcher.pushEvent(e) {
			goto DISPATCH_TASK
		}
	}
	return nil
}

func (c *EventDispatcher) peekEvent(rangeID uint64) RangeEvent {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.current_events[rangeID]
}

func (c *EventDispatcher) pushEvent(e RangeEvent) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	rangeID := e.GetRangeID()

	log.Debug("[range %d] add event: %v", rangeID, e)
	if old, ok := c.current_events[rangeID]; ok {
		log.Warn("[range %v] add event [%v] failure, old: %v", rangeID, e, old)
		return false
	}

	c.current_events[rangeID] = e
	return true
}

func (c *EventDispatcher) Run() {

}

func (c *EventDispatcher) Stop() {
	c.cancel()
	c.wg.Wait()
}

func (c *EventDispatcher) removeEvent(e RangeEvent) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.current_events, e.GetRangeID())
	c.cluster.metric.CollectEvent(e)
}


func (c *EventDispatcher) getEvents() []RangeEvent {
	c.lock.RLock()
	defer c.lock.RUnlock()

	events := make([]RangeEvent, 0)
	for _, e := range c.current_events {
		events = append(events, e)
	}

	return events
}
