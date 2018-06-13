package server

import "sync/atomic"

// RangeOpsStat range ops stat
type RangeOpsStat struct {
	writeOps [CacheSize]uint64
	hit      uint64
}

// Hit hit
func (opsStat *RangeOpsStat) Hit(v uint64) {
	hit := atomic.AddUint64(&(opsStat.hit), 1)
	opsStat.writeOps[hit%CacheSize] = v
}

// GetMax return max
func (opsStat *RangeOpsStat) GetMax() uint64 {
	var max uint64 = 0
	for i := 0; i < CacheSize; i++ {
		v := opsStat.writeOps[i]
		if v > max {
			max = v
		}
	}
	return max
}

// Clear clear
func (opsStat *RangeOpsStat) Clear() uint64 {
	var max uint64 = 0
	for i := 0; i < CacheSize; i++ {
		opsStat.writeOps[i] = 0
	}
	return max
}
