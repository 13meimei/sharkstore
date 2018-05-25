package model

import (
	"encoding/binary"
)

// Raft日志Key组成：
// localPrefixByte + LocalRangeIDPrefixByte + {RangeID} + localRangeIDUnreplicatedSuffixByte + {Raft类型} + [index如果有]

const (
	localPrefixByte = '\x01'

	localRangeIDPrefixByte = 'i'

	localRangeIDUnreplicatedSuffixByte = 'u'
)

var (
	// raft suffixs
	localRaftHardStateSuffix  = []byte("rfth")
	localRaftLastIndexSuffix  = []byte("rftl")
	localRaftApplyIndexSuffix = []byte("rfta")
	localRaftSnapshotMeta     = []byte("rfts")
	localRaftLogEntrySuffix   = []byte("rfte")
)

func makeRangeIDPrefix(rangeID uint64, suffix []byte) []byte {
	b := make([]byte, 2+8, 32)
	b[0] = localPrefixByte
	b[1] = localRangeIDPrefixByte
	binary.BigEndian.PutUint64(b[2:], rangeID) //TODO:优化，支持扫描的变长编码
	b = append(b, suffix...)
	return b
}

func MakeRangeIDUnreplicatedPrefix(rangeID uint64) []byte {
	return makeRangeIDPrefix(rangeID, []byte{localRangeIDUnreplicatedSuffixByte})
}

// RaftKeys
func makeRaftKey(rangeID uint64, suffix []byte) []byte {
	b := MakeRangeIDUnreplicatedPrefix(rangeID)
	b = append(b, suffix...)
	return b
}

func MakeRaftHardStateKey(rangeID uint64) []byte {
	return makeRaftKey(rangeID, localRaftHardStateSuffix)
}

func MakeRaftLastIndexKey(rangeID uint64) []byte {
	return makeRaftKey(rangeID, localRaftLastIndexSuffix)
}

func MakeRaftApplyIndexKey(rangeID uint64) []byte {
	return makeRaftKey(rangeID, localRaftApplyIndexSuffix)
}

func MakeRaftSnapshotMetaKey(rangeID uint64) []byte {
	return makeRaftKey(rangeID, localRaftSnapshotMeta)
}

func MakeRaftLogEntryKey(rangeID uint64, index uint64) []byte {
	b := makeRaftKey(rangeID, localRaftLogEntrySuffix)
	ib := make([]byte, 8)
	binary.BigEndian.PutUint64(ib, index) //TODO:优化，支持扫描的变长编码
	b = append(b, ib...)
	return b
}
