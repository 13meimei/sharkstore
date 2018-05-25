package server

import (
	"time"
	"fmt"
	"sync"

	"model/pkg/taskpb"
	"model/pkg/metapb"
	"util/log"
	"util/deepcopy"

	"pkg-go/ds_client"
	"errors"
)

type EventType int

const (
	EVENT_TYPE_INIT          = iota //永远不要被用到
	EVENT_TYPE_ADD_PEER
	EVENT_TYPE_DEL_PEER
	EVENT_TYPE_CHANGE_LEADER
	EVENT_TYPE_DEL_RANGE
)

func ToEventTypeName(eventType EventType) string {
	switch eventType {
	case EVENT_TYPE_INIT:
		return "init"
	case EVENT_TYPE_ADD_PEER:
		return "add peer"
	case EVENT_TYPE_DEL_PEER:
		return "del peer"
	case EVENT_TYPE_CHANGE_LEADER:
		return "change leader"
	case EVENT_TYPE_DEL_RANGE:
		return "del range"
	default:
		return "invalid type"
	}
}

type EventStatus int

const (
	EVENT_STATUS_INIT    EventStatus = iota //永远不要被用到
	EVENT_STATUS_CREATE
	EVENT_STATUS_DEALING
	EVENT_STATUS_FINISH
	EVENT_STATUS_TIMEOUT
	EVENT_STATUS_CANCEL
	EVENT_STATUS_FAILURE
)

func ToEventStatusName(status EventStatus) string {
	switch status {
	case EVENT_STATUS_INIT:
		return "init"
	case EVENT_STATUS_CREATE:
		return "create"
	case EVENT_STATUS_DEALING:
		return "deal..."
	case EVENT_STATUS_FINISH:
		return "finish"
	case EVENT_STATUS_TIMEOUT:
		return "timeout"
	case EVENT_STATUS_CANCEL:
		return "cancel"
	case EVENT_STATUS_FAILURE:
		return "failure"
	default:
		return "invalid status"
	}
}

type ExecNextEvent bool

type RangeEvent interface {
	IsTimeout() bool
	ExecTime() time.Duration
	String() string
	/**
	@param ExecNextEvent 为true表示要继续调用，如果为false需要判断整个事件是否close，以便把事件删除
	@param Task不为空，应该把ExecNextEvent设置为false
	为了管理端打印方便和执行的步骤，没有把指针设置为当前位置，通过状态跳转到下一个事件

	事件的执行时间需要快速完成，否则会阻塞心跳，事件执行时间过长会导致别的事件无法执行

 	*/
	Execute(cluster *Cluster, r *Range) (ExecNextEvent, *taskpb.Task, error)
	GetStatus() EventStatus
	IsClosed() bool
	GetRangeID() uint64
	Next() RangeEvent
	GetId() uint64
	GetType() EventType
}

type RangeEventMeta struct {
	id         uint64
	rangeId    uint64
	eventType  EventType
	creator    string
	nodeId     uint64
	status     EventStatus
	retryTimes int
	start      time.Time
	end        time.Time
	timeout    time.Duration
	task       *taskpb.Task
	next       RangeEvent
}

func NewRangeEvent(id, rangeId uint64, eventType EventType, timeout time.Duration, creator string, task *taskpb.Task) RangeEventMeta {
	return RangeEventMeta{id: id, rangeId: rangeId, eventType: eventType, status: EVENT_STATUS_CREATE, start: time.Now(),
		end: time.Now(), timeout: timeout, creator: creator, task: task}
}

func (m *RangeEventMeta) IsTimeout() bool {
	if m.status == EVENT_STATUS_TIMEOUT {
		return true
	}

	if time.Since(m.start) > m.timeout {
		return true
	}
	return false
}

func (m *RangeEventMeta) ExecTime() time.Duration {
	if m.end.IsZero() {
		return time.Now().Sub(m.start)
	}
	return m.end.Sub(m.start)
}

func (m *RangeEventMeta) GetStatus() EventStatus {
	return m.status
}

func (m *RangeEventMeta) GetType() EventType {
	return m.eventType
}

func (m *RangeEventMeta) GetRangeID() uint64 {
	return m.rangeId
}

func (m *RangeEventMeta) GetId() uint64 {
	return m.id
}

func (m *RangeEventMeta) Next() RangeEvent {
	return m.next
}

func (m *RangeEventMeta) String() string {
	str := fmt.Sprintf("[id:%d, type:%s, start:%s, exec time:%v s, rId:%v, creator:%s, status:%s, retry:%d]",
		m.id, ToEventTypeName(m.eventType), m.start.Format(DefaultTimeFormat), m.ExecTime().Seconds(), m.rangeId,
		m.creator, ToEventStatusName(m.status), m.retryTimes)

	if n := m.next; n != nil {
		str += n.String()
	}
	return str
}

func (m *RangeEventMeta) IsClosed() bool {
	if m.GetStatus() == EVENT_STATUS_FINISH {
		n := m.Next()
		for n != nil {
			if n.GetStatus() == EVENT_STATUS_FINISH {
				n = n.Next()
			} else if n.GetStatus() == EVENT_STATUS_TIMEOUT || n.GetStatus() == EVENT_STATUS_FAILURE {
				return true
			} else {
				return false
			}
		}
		return true

	} else if m.GetStatus() == EVENT_STATUS_TIMEOUT || m.GetStatus() == EVENT_STATUS_FAILURE {
		return true
	} else {
		return false
	}

}

type AddPeerEvent struct {
	RangeEventMeta
	lock sync.Mutex
	r    *Range
}

func NewAddPeerEvent(id, rangeId uint64, p *metapb.Peer, creator string) *AddPeerEvent {
	t := &taskpb.Task{
		Type: taskpb.TaskType_RangeAddPeer,

		RangeAddPeer: &taskpb.TaskRangeAddPeer{
			Peer: p,
		},
	}

	return &AddPeerEvent{RangeEventMeta: NewRangeEvent(id, rangeId, EVENT_TYPE_CHANGE_LEADER, DefaultAddPeerTimeout, creator, t)}
}

func (e *AddPeerEvent) Execute(cluster *Cluster, r *Range) (ExecNextEvent, *taskpb.Task, error) {
	/**
	不在锁里嵌套
	 */
	if e.GetStatus() == EVENT_STATUS_FINISH {
		if next := e.Next(); next != nil {
			return next.Execute(cluster, r)
		} else {
			return false, nil, nil
		}
	} else if e.GetStatus() == EVENT_STATUS_TIMEOUT || e.GetStatus() == EVENT_STATUS_FAILURE {
		return false, nil, nil
	}

	e.lock.Lock()
	defer e.lock.Unlock()
	if e.IsTimeout() {
		e.status = EVENT_STATUS_TIMEOUT
		return false, nil, nil
	}
	e.r = r.clone()
	e.end = time.Now()
	switch e.GetStatus() {
	case EVENT_STATUS_CREATE:
		//创建副本
		err := prepareAddPeer(cluster, r, e.task.GetRangeAddPeer().GetPeer())
		if err != nil {
			log.Warn("range remote create peer failed, err[%v]", err)
			e.status = EVENT_STATUS_FAILURE
			return false, nil, err
		}
		e.status = EVENT_STATUS_DEALING
		//把添加peer的任务下发给leader 等待心跳上报，peer是否添加成功
		return false, e.task, nil
	case EVENT_STATUS_DEALING:
		//添加成员还没有成功，再下发一次给leader,可能重复下发，DS需要防止重复
		if r.GetPeer(e.task.GetRangeAddPeer().GetPeer().GetId()) == nil {
			e.retryTimes += 1
			return false, e.task, nil
		}
		//还在同步数据，就再等等
		if r.GetPendingPeer(e.task.GetRangeAddPeer().GetPeer().GetId()) != nil {
			return false, nil, errors.New(fmt.Sprintf("wating for pending"))
		}
		e.status = EVENT_STATUS_FINISH
		return true, nil, nil
	default:
		log.Warn("AddPeerEvent: unknown status %v, ", ToEventStatusName(e.GetStatus()))
		return false, nil, errors.New(fmt.Sprintf("AddPeerEvent err status %s", ToEventStatusName(e.GetStatus())))
	}

}

type DelPeerEvent struct {
	RangeEventMeta
	lock sync.Mutex
	r    *Range
}

func NewDelPeerEvent(id, rangeId uint64, p *metapb.Peer, creator string) *DelPeerEvent {
	t := &taskpb.Task{
		Type: taskpb.TaskType_RangeDelPeer,
		RangeDelPeer: &taskpb.TaskRangeDelPeer{
			Peer: p,
		},
	}
	return &DelPeerEvent{RangeEventMeta: NewRangeEvent(id, rangeId, EVENT_TYPE_DEL_PEER, DefaultDelPeerTimeout, creator, t),}
}

func (e *DelPeerEvent) Execute(cluster *Cluster, r *Range) (ExecNextEvent, *taskpb.Task, error) {
	/**
	不在锁里嵌套
	 */
	if e.GetStatus() == EVENT_STATUS_FINISH {
		if next := e.Next(); next != nil {
			return next.Execute(cluster, r)
		} else {
			return false, nil, nil
		}
	} else if e.GetStatus() == EVENT_STATUS_TIMEOUT || e.GetStatus() == EVENT_STATUS_FAILURE {
		return false, nil, nil
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	if e.IsTimeout() {
		e.status = EVENT_STATUS_TIMEOUT
		return false, nil, nil
	}

	e.r = r.clone()
	e.end = time.Now()
	switch e.GetStatus() {
	case EVENT_STATUS_CREATE:
		e.status = EVENT_STATUS_DEALING
		return false, e.task, nil
	case EVENT_STATUS_DEALING:
		e.retryTimes += 1
		peer := e.task.GetRangeDelPeer().GetPeer()
		if r.GetPeer(peer.GetId()) == nil {
			peerGC(cluster, r.Range, peer)
			e.status = EVENT_STATUS_FINISH
			return true, nil, nil
		} else {
			//继续等待DS执行完
			return false, e.task, nil
		}
	default:
		return false, nil, errors.New(fmt.Sprintf("DelPeerEvent err status %s", ToEventStatusName(e.GetStatus())))
	}

}

type TryChangeLeaderEvent struct {
	RangeEventMeta
	lock      sync.Mutex
	preLeader *metapb.Peer
	expLeader *metapb.Peer
	r         *Range
}

func NewTryChangeLeaderEvent(id, rangeId uint64, preLeader, expLeader *metapb.Peer, creator string) *TryChangeLeaderEvent {
	t := &taskpb.Task{
		Type: taskpb.TaskType_RangeLeaderTransfer,
		RangeLeaderTransfer: &taskpb.TaskRangeLeaderTransfer{
			ExpLeader: expLeader,
		},
	}
	return &TryChangeLeaderEvent{RangeEventMeta: NewRangeEvent(id, rangeId, EVENT_TYPE_CHANGE_LEADER, DefaultChangeLeaderTimeout, creator, t),
		preLeader: preLeader, expLeader: expLeader,}
}

func (e *TryChangeLeaderEvent) Execute(cluster *Cluster, r *Range) (ExecNextEvent, *taskpb.Task, error) {
	/**
	不在锁里嵌套
	 */
	if e.GetStatus() == EVENT_STATUS_FINISH {
		if next := e.Next(); next != nil {
			return next.Execute(cluster, r)
		} else {
			return false, nil, nil
		}
	} else if e.GetStatus() == EVENT_STATUS_TIMEOUT || e.GetStatus() == EVENT_STATUS_FAILURE {
		return false, nil, nil
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	if e.IsTimeout() {
		e.status = EVENT_STATUS_TIMEOUT
		return false, nil, nil
	}

	e.r = r.clone()
	e.end = time.Now()
	switch e.GetStatus() {
	case EVENT_STATUS_CREATE:
		if r.GetLeader().GetNodeId() == e.expLeader.GetNodeId() {
			e.status = EVENT_STATUS_FINISH
			return true, nil, nil
		}
		// leader已经发生了改变
		if e.preLeader.GetNodeId() != r.GetLeader().GetNodeId() {
			e.status = EVENT_STATUS_FINISH
			return true, nil, nil
		}
		node := cluster.FindNodeById(e.expLeader.GetNodeId())
		//TODO:可能对堵塞时间比较长
		cluster.cli.TransferLeader(node.GetServerAddr(), r.GetId())
		e.status = EVENT_STATUS_DEALING
		return false, nil, nil
	case EVENT_STATUS_DEALING:
		e.retryTimes += 1
		if r.GetLeader().GetNodeId() == e.expLeader.GetNodeId() {
			e.status = EVENT_STATUS_FINISH
			return true, nil, nil
		}
		// 已经过了一个心跳周期，就强制做后续的任务，不需要做重试,认为成功
		e.status = EVENT_STATUS_FINISH
		return true, nil, nil
	default:
		return false, nil, errors.New(fmt.Sprintf("TryChangeLeaderEvent err status %s", ToEventStatusName(e.GetStatus())))

	}
}

type DelRangeEvent struct {
	RangeEventMeta
	lock sync.Mutex
	r    *Range
}

func (e *DelRangeEvent) Execute(cluster *Cluster, r *Range) (ExecNextEvent, *taskpb.Task, error) {
	/**
	不在锁里嵌套
	 */
	if e.GetStatus() == EVENT_STATUS_FINISH {
		if next := e.Next(); next != nil {
			return next.Execute(cluster, r)
		} else {
			return false, nil, nil
		}
	} else if e.GetStatus() == EVENT_STATUS_TIMEOUT || e.GetStatus() == EVENT_STATUS_FAILURE {
		return false, nil, nil
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	if e.IsTimeout() {
		e.status = EVENT_STATUS_TIMEOUT
		return false, nil, nil
	}

	e.r = r.clone()
	e.end = time.Now()
	switch e.GetStatus() {
	case EVENT_STATUS_CREATE:
		e.status = EVENT_STATUS_DEALING
		for _, peer := range r.Peers {
			node := cluster.FindNodeById(peer.GetNodeId())
			//TODO:可能对堵塞时间比较长
			err := cluster.cli.DeleteRange(node.GetServerAddr(), r.GetId())
			if err == nil {
				peerGC(cluster, r.Range, peer)
			}
		}
		return false, nil, errors.New("Waiting for deleteRange")
	case EVENT_STATUS_DEALING:
		e.retryTimes += 1
		for _, peer := range r.Peers {
			node := cluster.FindNodeById(peer.GetNodeId())
			//TODO:可能对堵塞时间比较长
			err := cluster.cli.DeleteRange(node.GetServerAddr(), r.GetId())
			if err == nil {
				peerGC(cluster, r.Range, peer)
			}
		}
		// 已经过了一个心跳周期，就强制做后续的任务，不需要做重试,认为成功
		e.status = EVENT_STATUS_FINISH
		return true, nil, nil
	default:
		return false, nil, errors.New(fmt.Sprintf("DelRangeEvent err status %s", ToEventStatusName(e.GetStatus())))

	}
}

func NewDelRangeEvent(id, rangeId uint64, creator string) *DelRangeEvent {
	return &DelRangeEvent{RangeEventMeta: NewRangeEvent(id, rangeId, EVENT_TYPE_DEL_RANGE, DefaultDelPeerTimeout, creator, nil),}
}

func prepareAddPeer(cluster *Cluster, r *Range, peer *metapb.Peer) error {
	rng := deepcopy.Iface(r.Range).(*metapb.Range)
	nodeId := peer.GetNodeId()
	node := cluster.FindNodeById(nodeId)
	if node == nil || !node.IsLogin() {
		return ErrRangeStatusErr
	}
	// 即使创建失败，raft leader添加成员
	var retry int = 3
	var err error
	for i := 0; i < retry; i++ {
		err = cluster.cli.CreateRange(node.GetServerAddr(), rng)
		if err == nil {
			break
		}
		// 链路问题
		if _, ok := err.(*client.RpcError); ok {
			break
		}
		if i+1 < retry {
			time.Sleep(time.Millisecond * time.Duration(10*(i+1)))
		}
	}

	return err
}

func NewChangePeerEvent(id uint64, rng *Range, oldPeer, newPeer *metapb.Peer, creator string) RangeEvent {
	addPeer := NewAddPeerEvent(id, rng.GetId(), newPeer, creator)
	removePeer := NewDelPeerEvent(id, rng.GetId(), oldPeer, creator)
	if rng.GetLeader() != nil && rng.GetLeader().GetId() == oldPeer.GetId() {
		newLeader := newPeer
		if follower := rng.GetRandomFollower(); follower != nil {
			newLeader = follower
		}
		transferLeader := NewTryChangeLeaderEvent(id, rng.GetId(), rng.GetLeader(), newLeader, creator)
		addPeer.next = transferLeader
		transferLeader.next = removePeer
		return addPeer
	}
	addPeer.next = removePeer
	return addPeer
}
