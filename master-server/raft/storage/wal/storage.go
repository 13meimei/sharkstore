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

package wal

import (
	"fmt"
	"time"

	"master-server/raft/logger"
	"master-server/raft/proto"
	"util/log"
)

// Storage the storage
type Storage struct {
	id uint64
	c  *Config

	// Log Entry
	ls         *logEntryStorage
	truncIndex uint64
	truncTerm  uint64

	hardState  proto.HardState
	metafile   *metaFile
	prevCommit uint64 // 有commit变化时sync一下

	closed bool
}

// NewStorage new
func NewStorage(id uint64, dir string, c *Config) (*Storage, error) {
	if err := initDir(dir); err != nil {
		return nil, err
	}

	// 加载HardState
	mf, hardState, meta, err := openMetaFile(dir)
	if err != nil {
		return nil, err
	}

	log.Info("wal[%d] open commit: %d, term: %d, truncIndex: %d, truncTerm: %d", id, hardState.Commit, hardState.Term, meta.truncIndex, meta.truncTerm)

	s := &Storage{
		c:          c.dup(),
		id:         id,
		truncIndex: meta.truncIndex,
		truncTerm:  meta.truncTerm,
		hardState:  hardState,
		metafile:   mf,
		prevCommit: hardState.Commit,
	}

	// 加载日志文件
	ls, err := openLogStorage(dir, s)
	if err != nil {
		return nil, err
	}
	s.ls = ls

	if c.GetTruncateFirstDummy() {
		if err := s.truncateFirstDummy(); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *Storage) truncateFirstDummy() error {
	// 保证是初始化时（不能已有日志存在）
	li, err := s.LastIndex()
	if err != nil {
		return err
	}
	if li != 0 {
		log.Warn("wal[%d] truncate first dummy fobbiden(not empty), last index: %d", s.id, li)
		return nil
	}

	meta := truncateMeta{
		truncIndex: 1,
		truncTerm:  1,
	}

	if err = s.metafile.SaveTruncateMeta(meta); err != nil {
		return err
	}
	if err = s.metafile.Sync(); err != nil {
		return err
	}

	s.truncIndex = meta.truncIndex
	s.truncTerm = meta.truncTerm

	return nil
}

// InitialState returns the saved HardState information to init the repl state.
func (s *Storage) InitialState() (proto.HardState, error) {
	return s.hardState, nil
}

// Entries returns a slice of log entries in the range [lo,hi), the hi is not inclusive.
// MaxSize limits the total size of the log entries returned, but Entries returns at least one entry if any.
// If lo <= CompactIndex,then return isCompact true.
// If no entries,then return entries nil.
// Note: math.MaxUint32 is no limit.
func (s *Storage) Entries(lo, hi uint64, maxSize uint64) (entries []*proto.Entry, isCompact bool, err error) {
	if lo <= s.truncIndex {
		return nil, true, nil
	}
	entries, isCompact, err = s.ls.Entries(lo, hi, maxSize)
	return
}

// Term returns the term of entry i, which must be in the range [FirstIndex()-1, LastIndex()].
// The term of the entry before FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
// If lo <= CompactIndex,then return isCompact true.
func (s *Storage) Term(index uint64) (term uint64, isCompact bool, err error) {
	switch {
	case index < s.truncIndex:
		return 0, true, nil
	case index == s.truncIndex:
		term = s.truncTerm
		return
	default:
		term, isCompact, err = s.ls.Term(index)
		return
	}
}

// FirstIndex returns the index of the first log entry that is possibly available via Entries (older entries have been incorporated
// into the latest Snapshot; if storage only contains the dummy entry the first log entry is not available).
func (s *Storage) FirstIndex() (index uint64, err error) {
	index = s.truncIndex + 1
	return
}

// LastIndex returns the index of the last entry in the log.
func (s *Storage) LastIndex() (index uint64, err error) {
	index = s.ls.LastIndex()
	if index < s.truncIndex {
		index = s.truncIndex
	}
	return
}

// StoreEntries store the log entries to the repository.
// If first index of entries > LastIndex,then append all entries,
// Else write entries at first index and truncate the redundant log entries.
func (s *Storage) StoreEntries(entries []*proto.Entry) error {
	if err := s.ls.SaveEntries(entries); err != nil {
		return err
	}
	return nil
}

// StoreHardState store the raft state to the repository.
func (s *Storage) StoreHardState(st proto.HardState) error {
	if err := s.metafile.SaveHardState(st); err != nil {
		return err
	}
	s.hardState = st

	if s.c.GetSync() {
		sync := false
		if st.Commit != s.prevCommit {
			sync = true
			s.prevCommit = st.Commit
		}
		if sync {
			if err := s.metafile.Sync(); err != nil {
				return err
			}
			if err := s.ls.Sync(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Truncate the log to index,  The index is inclusive.
func (s *Storage) Truncate(index uint64) error {
	if index <= s.truncIndex {
		log.Warn("wal[%d] already truncated. index=%d, trunc=%d", s.id, index, s.truncIndex)
		return nil
	}

	log.Info("wal[%d] log truncate. index=%d", s.id, index)

	term, isCompact, err := s.ls.Term(index)
	if err != nil {
		return err
	}
	if isCompact {
		return fmt.Errorf("expected compacted term. index:%d", index)
	}

	// 更新meta
	meta := truncateMeta{
		truncIndex: index,
		truncTerm:  term,
	}
	if err = s.metafile.SaveTruncateMeta(meta); err != nil {
		return err
	}
	if err = s.metafile.Sync(); err != nil {
		return err
	}

	// 截断日志文件
	if err = s.ls.TruncateFront(index); err != nil {
		return err
	}

	s.truncIndex = index
	s.truncTerm = term

	return nil
}

// ApplySnapshot Sync snapshot status.
func (s *Storage) ApplySnapshot(meta proto.SnapshotMeta) error {
	tMeta := truncateMeta{
		truncIndex: meta.Index,
		truncTerm:  meta.Term,
	}

	log.Info("wal[%d] apply snapshot. meta: %v", s.id, meta)

	var err error

	// 更新commit位置
	s.hardState.Commit = meta.Index
	if err := s.metafile.SaveHardState(s.hardState); err != nil {
		return err
	}

	// truncate meta
	if err = s.metafile.SaveTruncateMeta(tMeta); err != nil {
		return err
	}
	if err = s.metafile.Sync(); err != nil {
		return err
	}

	if err = s.ls.TruncateAll(meta.Index + 1); err != nil {
		return err
	}

	s.truncIndex = meta.Index
	s.truncTerm = meta.Term

	return nil
}

// Close the storage.
func (s *Storage) Close() {
	if !s.closed {
		s.ls.Close()
		s.metafile.Close()
		s.closed = true
	}
}

type metricReporter struct {
	ID string
}

func newReporterWithID(id string) *metricReporter {
	return &metricReporter{
		ID: id,
	}
}

func (r *metricReporter) ReportInterval() time.Duration {
	return time.Minute
}

func (r *metricReporter) Report(data []byte) error {
	logger.Info("wal [%s] metrics: %s", r.ID, string(data))
	return nil
}
