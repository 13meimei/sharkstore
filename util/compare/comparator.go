package compare

import (
	"bytes"
	"time"
)

// Compare returns a value indicating the sort order relationship between the
// receiver and the parameter.
// Given c = a.Compare(b):
//  c < 0 if a < b;
//  c == 0 if a == b; and
//  c > 0 if a > b.
type Comparable interface {
	Compare(Comparable) int
}

type CompString string

func (a CompString) Compare(b Comparable) int {
	aAsserted := string(a)
	bAsserted := string(b.(CompString))
	min := len(bAsserted)
	if len(aAsserted) < len(bAsserted) {
		min = len(aAsserted)
	}
	diff := 0
	for i := 0; i < min && diff == 0; i++ {
		diff = int(aAsserted[i]) - int(bAsserted[i])
	}
	if diff == 0 {
		diff = len(aAsserted) - len(bAsserted)
	}
	if diff < 0 {
		return -1
	}
	if diff > 0 {
		return 1
	}
	return 0
}

type CompInt int

func (a CompInt) Compare(b Comparable) int {
	aAsserted := int(a)
	bAsserted := int(b.(CompInt))
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

type CompInt8 int8

func (a CompInt8) Compare(b Comparable) int {
	aAsserted := int8(a)
	bAsserted := int8(b.(CompInt8))
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

type CompInt16 int16

func (a CompInt16) Compare(b Comparable) int {
	aAsserted := int16(a)
	bAsserted := int16(b.(CompInt16))
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

type CompInt32 int32

func (a CompInt32) Compare(b Comparable) int {
	aAsserted := int32(a)
	bAsserted := int32(b.(CompInt32))
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

type CompInt64 int64

func (a CompInt64) Compare(b Comparable) int {
	aAsserted := int64(a)
	bAsserted := int64(b.(CompInt64))
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

type CompUint uint

func (a CompUint) Compare(b Comparable) int {
	aAsserted := uint(a)
	bAsserted := uint(b.(CompUint))
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

type CompUint8 uint8

func (a CompUint8) Compare(b Comparable) int {
	aAsserted := uint8(a)
	bAsserted := uint8(b.(CompUint8))
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

type CompUint16 uint16

func (a CompUint16) Compare(b Comparable) int {
	aAsserted := uint16(a)
	bAsserted := uint16(b.(CompUint16))
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

type CompUint32 uint32

func (a CompUint32) Compare(b Comparable) int {
	aAsserted := uint32(a)
	bAsserted := uint32(b.(CompUint32))
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

type CompUint64 uint64

func (a CompUint64) Compare(b Comparable) int {
	aAsserted := uint64(a)
	bAsserted := uint64(b.(CompUint64))
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

type CompFloat32 float32

func (a CompFloat32) Compare(b Comparable) int {
	aAsserted := float32(a)
	bAsserted := float32(b.(CompFloat32))
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

type CompFloat64 float64

func (a CompFloat64) Compare(b Comparable) int {
	aAsserted := float64(a)
	bAsserted := float64(b.(CompFloat64))
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

type CompByte byte

func (a CompByte) Compare(b Comparable) int {
	aAsserted := byte(a)
	bAsserted := byte(b.(CompByte))
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

type CompRune rune

func (a CompRune) Compare(b Comparable) int {
	aAsserted := rune(a)
	bAsserted := rune(b.(CompRune))
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

type CompTime time.Time

func (a CompTime) Compare(b Comparable) int {
	aAsserted := time.Time(a)
	bAsserted := time.Time(b.(CompTime))

	switch {
	case aAsserted.After(bAsserted):
		return 1
	case aAsserted.Before(bAsserted):
		return -1
	default:
		return 0
	}
}

type CompBytes []byte

func (a CompBytes) Compare(b Comparable) int {
	aAsserted := []byte(a)
	bAsserted := []byte(b.(CompBytes))

	return bytes.Compare(aAsserted, bAsserted)
}
