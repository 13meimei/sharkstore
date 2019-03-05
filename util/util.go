package util

import (
	"fmt"
	"math/rand"
	"sort"
	"bytes"
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)

const TTL_COL_NAME string = "ttl"

type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Sort is a convenience method.
func (p Int64Slice) Sort() { sort.Sort(p) }


var bunits = [...]string{"", "Ki", "Mi", "Gi"}

// ShorteNBytes
func ShorteNBytes(bytes int) string {
	i := 0
	for ; bytes > 1024 && i < 4; i++ {
		bytes /= 1024
	}
	return fmt.Sprintf("%d%sB", bytes, bunits[i])
}

// MinInt min
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ManInt max
func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

var randomBaseBytes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// RandomBytes random bytes
func RandomBytes(r *rand.Rand, dst []byte) {
	for i := 0; i < len(dst); i++ {
		dst[i] = byte(randomBaseBytes[r.Intn(len(randomBaseBytes))])
	}
}

type Range struct {
	Start []byte
	Limit []byte
}

func BytesPrefix(prefix []byte) *Range {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return &Range{prefix, limit}
}

type KeyPair struct {
	StartKey []byte
	EndKey   []byte
}

type KeyPairSlice []KeyPair

func (slice KeyPairSlice) Len() int { return len(slice) }
func (slice KeyPairSlice) Less(i, j int) bool {
	if bytes.Compare(slice[i].EndKey, slice[j].StartKey) < 0 {
		return true
	}
	if bytes.Compare(slice[i].StartKey, slice[j].StartKey) < 0 {
		return true
	}
	if bytes.Compare(slice[i].EndKey, slice[j].EndKey) < 0 {
		return true
	}
	return false
}
func (slice KeyPairSlice) Swap(i, j int) { slice[i], slice[j] = slice[j], slice[i] }

// Sort is a convenience method.
func (slice KeyPairSlice) Sort() { sort.Sort(slice) }

func (slice KeyPairSlice) Get(i int) KeyPair {
	return slice[i]
}