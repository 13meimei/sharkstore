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

package main

import (
	"encoding/binary"
	"io"
)

type snapshot struct {
	sum        uint64
	applied    uint64
	nextCalled bool
}

func newSnapshot(sum uint64, applied uint64) *snapshot {
	return &snapshot{
		sum:     sum,
		applied: applied,
	}
}

func (s *snapshot) ApplyIndex() uint64 { return s.applied }

func (s *snapshot) Close() {}

func (s *snapshot) Next() ([]byte, error) {
	if s.nextCalled {
		return nil, io.EOF
	}
	s.nextCalled = true
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, s.sum)
	return buf, nil
}
