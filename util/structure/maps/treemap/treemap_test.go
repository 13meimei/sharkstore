package treemap

import (
	"fmt"
	"testing"

	"util/compare"
)

var testSafe = false

func TestMapPut(t *testing.T) {
	m := New(testSafe)
	m.Put(compare.CompInt(5), "e")
	m.Put(compare.CompInt(6), "f")
	m.Put(compare.CompInt(7), "g")
	m.Put(compare.CompInt(3), "c")
	m.Put(compare.CompInt(4), "d")
	m.Put(compare.CompInt(1), "x")
	m.Put(compare.CompInt(2), "b")
	m.Put(compare.CompInt(1), "a") //overwrite

	if actualValue := m.Size(); actualValue != 7 {
		t.Errorf("Got %v expected %v", actualValue, 7)
	}
	if actualValue, expectedValue := m.Keys(), []compare.Comparable{compare.CompInt(1), compare.CompInt(2), compare.CompInt(3), compare.CompInt(4), compare.CompInt(5), compare.CompInt(6), compare.CompInt(7)}; !sameElemKeys(actualValue, expectedValue) {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := m.Values(), []interface{}{"a", "b", "c", "d", "e", "f", "g"}; !sameElemValues(actualValue, expectedValue) {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}

	// key,expectedValue,expectedFound
	tests1 := [][]interface{}{
		{compare.CompInt(1), "a", true},
		{compare.CompInt(2), "b", true},
		{compare.CompInt(3), "c", true},
		{compare.CompInt(4), "d", true},
		{compare.CompInt(5), "e", true},
		{compare.CompInt(6), "f", true},
		{compare.CompInt(7), "g", true},
		{compare.CompInt(8), nil, false},
	}

	for _, test := range tests1 {
		// retrievals
		actualValue, actualFound := m.Get(test[0].(compare.CompInt))
		if actualValue != test[1] || actualFound != test[2] {
			t.Errorf("Got %v expected %v", actualValue, test[1])
		}
	}
}

func TestMapRemove(t *testing.T) {
	m := New(testSafe)
	m.Put(compare.CompInt(5), "e")
	m.Put(compare.CompInt(6), "f")
	m.Put(compare.CompInt(7), "g")
	m.Put(compare.CompInt(3), "c")
	m.Put(compare.CompInt(4), "d")
	m.Put(compare.CompInt(1), "x")
	m.Put(compare.CompInt(2), "b")
	m.Put(compare.CompInt(1), "a") //overwrite

	m.Remove(compare.CompInt(5))
	m.Remove(compare.CompInt(6))
	m.Remove(compare.CompInt(7))
	m.Remove(compare.CompInt(8))
	m.Remove(compare.CompInt(5))

	if actualValue, expectedValue := m.Keys(), []compare.Comparable{compare.CompInt(1), compare.CompInt(2), compare.CompInt(3), compare.CompInt(4)}; !sameElemKeys(actualValue, expectedValue) {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}

	if actualValue, expectedValue := m.Values(), []interface{}{"a", "b", "c", "d"}; !sameElemValues(actualValue, expectedValue) {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue := m.Size(); actualValue != 4 {
		t.Errorf("Got %v expected %v", actualValue, 4)
	}

	tests2 := [][]interface{}{
		{compare.CompInt(1), "a", true},
		{compare.CompInt(2), "b", true},
		{compare.CompInt(3), "c", true},
		{compare.CompInt(4), "d", true},
		{compare.CompInt(5), nil, false},
		{compare.CompInt(6), nil, false},
		{compare.CompInt(7), nil, false},
		{compare.CompInt(8), nil, false},
	}

	for _, test := range tests2 {
		actualValue, actualFound := m.Get(test[0].(compare.CompInt))
		if actualValue != test[1] || actualFound != test[2] {
			t.Errorf("Got %v expected %v", actualValue, test[1])
		}
	}

	m.Remove(compare.CompInt(1))
	m.Remove(compare.CompInt(4))
	m.Remove(compare.CompInt(2))
	m.Remove(compare.CompInt(3))
	m.Remove(compare.CompInt(2))
	m.Remove(compare.CompInt(2))

	if actualValue, expectedValue := fmt.Sprintf("%s", m.Keys()), "[]"; actualValue != expectedValue {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := fmt.Sprintf("%s", m.Values()), "[]"; actualValue != expectedValue {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue := m.Size(); actualValue != 0 {
		t.Errorf("Got %v expected %v", actualValue, 0)
	}
	if actualValue := m.Empty(); actualValue != true {
		t.Errorf("Got %v expected %v", actualValue, true)
	}
}

func sameElemKeys(a []compare.Comparable, b []compare.Comparable) bool {
	if len(a) != len(b) {
		return false
	}
	for _, av := range a {
		found := false
		for _, bv := range b {
			if av == bv {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func sameElemValues(a []interface{}, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for _, av := range a {
		found := false
		for _, bv := range b {
			if av == bv {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func benchmarkGet(b *testing.B, m *Map, size int) {
	for i := 0; i < b.N; i++ {
		for n := 0; n < size; n++ {
			m.Get(compare.CompInt(n))
		}
	}
}

func benchmarkPut(b *testing.B, m *Map, size int) {
	for i := 0; i < b.N; i++ {
		for n := 0; n < size; n++ {
			m.Put(compare.CompInt(n), struct{}{})
		}
	}
}

func benchmarkRemove(b *testing.B, m *Map, size int) {
	for i := 0; i < b.N; i++ {
		for n := 0; n < size; n++ {
			m.Remove(compare.CompInt(n))
		}
	}
}

func BenchmarkTreeMapGet100(b *testing.B) {
	b.StopTimer()
	size := 100
	m := New(testSafe)
	for n := 0; n < size; n++ {
		m.Put(compare.CompInt(n), struct{}{})
	}
	b.StartTimer()
	benchmarkGet(b, m, size)
}

func BenchmarkTreeMapGet1000(b *testing.B) {
	b.StopTimer()
	size := 1000
	m := New(testSafe)
	for n := 0; n < size; n++ {
		m.Put(compare.CompInt(n), struct{}{})
	}
	b.StartTimer()
	benchmarkGet(b, m, size)
}

func BenchmarkTreeMapGet10000(b *testing.B) {
	b.StopTimer()
	size := 10000
	m := New(testSafe)
	for n := 0; n < size; n++ {
		m.Put(compare.CompInt(n), struct{}{})
	}
	b.StartTimer()
	benchmarkGet(b, m, size)
}

func BenchmarkTreeMapGet100000(b *testing.B) {
	b.StopTimer()
	size := 100000
	m := New(testSafe)
	for n := 0; n < size; n++ {
		m.Put(compare.CompInt(n), struct{}{})
	}
	b.StartTimer()
	benchmarkGet(b, m, size)
}

func BenchmarkTreeMapPut100(b *testing.B) {
	b.StopTimer()
	size := 100
	m := New(testSafe)
	b.StartTimer()
	benchmarkPut(b, m, size)
}

func BenchmarkTreeMapPut1000(b *testing.B) {
	b.StopTimer()
	size := 1000
	m := New(testSafe)
	for n := 0; n < size; n++ {
		m.Put(compare.CompInt(n), struct{}{})
	}
	b.StartTimer()
	benchmarkPut(b, m, size)
}

func BenchmarkTreeMapPut10000(b *testing.B) {
	b.StopTimer()
	size := 10000
	m := New(testSafe)
	for n := 0; n < size; n++ {
		m.Put(compare.CompInt(n), struct{}{})
	}
	b.StartTimer()
	benchmarkPut(b, m, size)
}

func BenchmarkTreeMapPut100000(b *testing.B) {
	b.StopTimer()
	size := 100000
	m := New(testSafe)
	for n := 0; n < size; n++ {
		m.Put(compare.CompInt(n), struct{}{})
	}
	b.StartTimer()
	benchmarkPut(b, m, size)
}

func BenchmarkTreeMapRemove100(b *testing.B) {
	b.StopTimer()
	size := 100
	m := New(testSafe)
	for n := 0; n < size; n++ {
		m.Put(compare.CompInt(n), struct{}{})
	}
	b.StartTimer()
	benchmarkRemove(b, m, size)
}

func BenchmarkTreeMapRemove1000(b *testing.B) {
	b.StopTimer()
	size := 1000
	m := New(testSafe)
	for n := 0; n < size; n++ {
		m.Put(compare.CompInt(n), struct{}{})
	}
	b.StartTimer()
	benchmarkRemove(b, m, size)
}

func BenchmarkTreeMapRemove10000(b *testing.B) {
	b.StopTimer()
	size := 10000
	m := New(testSafe)
	for n := 0; n < size; n++ {
		m.Put(compare.CompInt(n), struct{}{})
	}
	b.StartTimer()
	benchmarkRemove(b, m, size)
}

func BenchmarkTreeMapRemove100000(b *testing.B) {
	b.StopTimer()
	size := 100000
	m := New(testSafe)
	for n := 0; n < size; n++ {
		m.Put(compare.CompInt(n), struct{}{})
	}
	b.StartTimer()
	benchmarkRemove(b, m, size)
}
