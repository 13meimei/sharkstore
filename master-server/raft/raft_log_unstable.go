package raft

import (
	"fmt"

	"master-server/raft/logger"
	"master-server/raft/proto"
)

// unstable temporary deposit the unpersistent log entries.It has log position i+unstable.offset.
// unstable can support group commit.
// Note that unstable.offset may be less than the highest log position in storage;
// this means that the next write to storage might need to truncate the log before persisting unstable.entries.
type unstable struct {
	offset uint64
	// all entries that have not yet been written to storage.
	entries []*proto.Entry
}

// maybeLastIndex returns the last index if it has at least one unstable entry.
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	return 0, false
}

// myabeTerm returns the term of the entry at index i, if there is any.
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		return 0, false
	}

	last, ok := u.maybeLastIndex()
	if !ok || i > last {
		return 0, false
	}
	return u.entries[i-u.offset].Term, true
}

func (u *unstable) stableTo(i, t uint64) {
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	if gt == t && i >= u.offset {
		l := uint64(len(u.entries))
		diff := l - (i + 1 - u.offset)
		if diff > 0 {
			copy(u.entries, u.entries[i+1-u.offset:l])
		}
		for k := diff; k < l; k++ {
			u.entries[k] = nil
		}
		u.entries = u.entries[0:diff]
		u.offset = i + 1
	}
}

func (u *unstable) restore(index uint64) {
	for i, l := 0, len(u.entries); i < l; i++ {
		u.entries[i] = nil
	}
	u.entries = u.entries[0:0]
	u.offset = index + 1
}

func (u *unstable) truncateAndAppend(ents []*proto.Entry) {
	after := ents[0].Index
	switch {
	case after == u.offset+uint64(len(u.entries)):
		// after is the next index in the u.entries directly append
		u.entries = append(u.entries, ents...)

	case after <= u.offset:
		// The log is being truncated to before our current offset portion, so set the offset and replace the entries
		for i, l := 0, len(u.entries); i < l; i++ {
			u.entries[i] = nil
		}
		u.entries = append(u.entries[0:0], ents...)
		u.offset = after

	default:
		// truncate to after and copy to u.entries then append
		u.entries = append(u.entries[0:0], u.slice(u.offset, after)...)
		u.entries = append(u.entries, ents...)
	}
}

func (u *unstable) slice(lo uint64, hi uint64) []*proto.Entry {
	u.mustCheckOutOfBounds(lo, hi)
	return u.entries[lo-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.offset)
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		errMsg := fmt.Sprintf("unstable.slice[%d,%d) is invalid.", lo, hi)
		logger.Error(errMsg)
		panic(AppPanicError(errMsg))
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		errMsg := fmt.Sprintf("unstable.slice[%d,%d) out of bound [%d,%d].", lo, hi, u.offset, upper)
		logger.Error(errMsg)
		panic(AppPanicError(errMsg))
	}
}
