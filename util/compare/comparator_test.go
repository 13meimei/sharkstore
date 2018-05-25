package compare

import (
	"testing"
	"time"
)

func TestIntComparator(t *testing.T) {

	// i1,i2,expected
	tests := [][]interface{}{
		{CompInt(1), CompInt(1), 0},
		{CompInt(1), CompInt(2), -1},
		{CompInt(2), CompInt(1), 1},
		{CompInt(11), CompInt(22), -1},
		{CompInt(0), CompInt(0), 0},
		{CompInt(1), CompInt(0), 1},
		{CompInt(0), CompInt(1), -1},
	}

	for _, test := range tests {
		actual := (test[0].(CompInt)).Compare(test[1].(CompInt))
		expected := test[2]
		if actual != expected {
			t.Errorf("Got %v expected %v", actual, expected)
		}
	}
}

func TestStringComparator(t *testing.T) {

	// s1,s2,expected
	tests := [][]interface{}{
		{CompString("a"), CompString("a"), 0},
		{CompString("a"), CompString("b"), -1},
		{CompString("b"), CompString("a"), 1},
		{CompString("aa"), CompString("aab"), -1},
		{CompString(""), CompString(""), 0},
		{CompString("a"), CompString(""), 1},
		{CompString(""), CompString("a"), -1},
		{CompString(""), CompString("aaaaaaa"), -1},
	}

	for _, test := range tests {
		actual := (test[0].(CompString)).Compare(test[1].(CompString))
		expected := test[2]
		if actual != expected {
			t.Errorf("Got %v expected %v", actual, expected)
		}
	}
}

func TestTimeComparator(t *testing.T) {

	now := time.Now()

	// i1,i2,expected
	tests := [][]interface{}{
		{now, now, 0},
		{now.Add(24 * 7 * 2 * time.Hour), now, 1},
		{now, now.Add(24 * 7 * 2 * time.Hour), -1},
	}

	for _, test := range tests {
		actual := CompTime(test[0].(time.Time)).Compare(CompTime(test[1].(time.Time)))
		expected := test[2]
		if actual != expected {
			t.Errorf("Got %v expected %v", actual, expected)
		}
	}
}

type custom struct {
	id   int
	name string
}

func (a custom) Compare(b Comparable) int {
	c1 := a
	c2 := b.(custom)
	switch {
	case c1.id > c2.id:
		return 1
	case c1.id < c2.id:
		return -1
	default:
		return 0
	}
}

func TestCustomComparator(t *testing.T) {
	tests := [][]interface{}{
		{custom{1, "a"}, custom{1, "a"}, 0},
		{custom{1, "a"}, custom{2, "b"}, -1},
		{custom{2, "b"}, custom{1, "a"}, 1},
		{custom{1, "a"}, custom{1, "b"}, 0},
	}

	for _, test := range tests {
		actual := (test[0].(custom)).Compare(test[1].(custom))
		expected := test[2]
		if actual != expected {
			t.Errorf("Got %v expected %v", actual, expected)
		}
	}
}
