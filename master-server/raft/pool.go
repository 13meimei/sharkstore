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

import (
	"sync"
)

var pool = newPoolFactory()

type poolFactory struct {
	applyPool    *sync.Pool
	proposalPool *sync.Pool
}

func newPoolFactory() *poolFactory {
	return &poolFactory{
		applyPool: &sync.Pool{
			New: func() interface{} {
				return new(apply)
			},
		},

		proposalPool: &sync.Pool{
			New: func() interface{} {
				return new(proposal)
			},
		},
	}
}

func (f *poolFactory) getApply() *apply {
	a := f.applyPool.Get().(*apply)
	a.command = nil
	a.future = nil
	return a
}

func (f *poolFactory) returnApply(a *apply) {
	if a != nil {
		f.applyPool.Put(a)
	}
}

func (f *poolFactory) getProposal() *proposal {
	p := f.proposalPool.Get().(*proposal)
	p.data = nil
	p.future = nil
	return p
}

func (f *poolFactory) returnProposal(p *proposal) {
	if p != nil {
		f.proposalPool.Put(p)
	}
}
