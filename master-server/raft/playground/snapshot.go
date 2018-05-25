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
