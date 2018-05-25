package context

import (
	"errors"
	"time"
)

// ErrCtxTimeout timeout
var ErrCtxTimeout = errors.New("fbase context timeout")

type TimerCtx struct {
	timer *time.Timer
	// closed by net closed
	done     chan struct{}
	deadline time.Time
}

func NewTimerCtx(timeout time.Duration) *TimerCtx {
	return &TimerCtx{
		timer:    time.NewTimer(timeout),
		done:     make(chan struct{}),
		deadline: time.Now().Add(timeout)}
}

func (cc *TimerCtx) Deadline() (deadline time.Time, ok bool) {
	return cc.deadline, true
}

func (cc *TimerCtx) Done() <-chan struct{} {
	select {
	// 如果已经close，直接返回
	case <-cc.done:
		return cc.done
	// 检查链路是否已经close
	case <-cc.timer.C:
		close(cc.done)
	default:
		return cc.done
	}
	return cc.done
}

func (cc *TimerCtx) Err() error {
	select {
	case <-cc.done:
		return ErrCtxTimeout
	case <-cc.timer.C:
		return ErrCtxTimeout
	default:
		return nil
	}
}

func (cc *TimerCtx) Value(key interface{}) interface{} {
	return nil
}

func (cc *TimerCtx) Reset(timeout time.Duration) {
	if cc.timer == nil {
		cc.timer = time.NewTimer(timeout)
		cc.deadline = time.Now().Add(timeout)
	} else {
		if !cc.timer.Stop() {
			<-cc.timer.C
		}
		cc.timer.Reset(timeout)
		cc.deadline = time.Now().Add(timeout)
	}
	cc.done = make(chan struct{})
}

func (cc *TimerCtx) Close() {
	if cc.timer != nil {
		if !cc.timer.Stop() {
			<-cc.timer.C
		}
	}
}
