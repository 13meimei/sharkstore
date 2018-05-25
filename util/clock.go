package util

type Clocker interface {
	Now() uint64
}

type incrementSeqClock struct {
	seq uint64
}

func NewIncrementSeqClock() Clocker {
	return &incrementSeqClock{}
}

// Now not thread-safety
func (c *incrementSeqClock) Now() uint64 {
	c.seq++
	return c.seq
}
