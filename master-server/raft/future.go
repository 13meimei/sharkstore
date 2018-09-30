// Copyright 2018 The TigLabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
