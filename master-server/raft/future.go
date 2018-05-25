package raft

import "context"

type deferError struct {
	errCh chan error
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) respond(err error) {
	d.errCh <- err
	close(d.errCh)
}

func (d *deferError) error() <-chan error {
	return d.errCh
}

type Future struct {
	deferError
	ctx    context.Context
	respCh chan interface{}
}

func newFuture(ctx context.Context) *Future {
	f := &Future{
		ctx:    ctx,
		respCh: make(chan interface{}, 1),
	}
	f.init()
	return f
}

func (f *Future) respond(resp interface{}, err error) {
	f.deferError.respond(err)
	if err == nil {
		f.respCh <- resp
	}
	close(f.respCh)
}

func (f *Future) Response() (resp interface{}, err error) {
	// wait error
	select {
	case err = <-f.error():
	case <-f.ctx.Done():
		return nil, ErrCanceled
	}
	if err != nil {
		return
	}
	// wait resp
	select {
	case resp = <-f.respCh:
		return
	case <-f.ctx.Done():
		return nil, ErrCanceled
	}
}
