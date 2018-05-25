package llrbtree

import (
	"testing"

	"github.com/google/btree"
)

// Benchmarks btree
type intItem int

func (a intItem) Less(than btree.Item) bool {
	return a < than.(intItem)
}

type intItemUpper int

func (ci intItemUpper) Less(i btree.Item) bool {
	var c int
	switch i := i.(type) {
	case intItemUpper:
		c = int(ci) - int(i)
	case intItem:
		c = int(ci) - int(i)
	}

	return c < 0
}

func BenchmarkBTreeInsert(b *testing.B) {
	t := btree.New(4)
	for i := 0; i < b.N; i++ {
		t.ReplaceOrInsert(intItem(b.N - i))
	}
}

func BenchmarkBTreeInsertNoRep(b *testing.B) {
	t := btree.New(4)
	for i := 0; i < b.N; i++ {
		t.ReplaceOrInsert(intItemUpper(b.N - i))
	}
}

func BenchmarkBTreeGet(b *testing.B) {
	b.StopTimer()
	t := btree.New(4)
	for i := 0; i < b.N; i++ {
		t.ReplaceOrInsert(intItem(b.N - i))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		t.Get(intItem(i))
	}
}

func BenchmarkBtreeMin(b *testing.B) {
	b.StopTimer()
	t := btree.New(4)
	for i := 0; i < 1e5; i++ {
		t.ReplaceOrInsert(intItem(i))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		m := t.Min()
		_ = m
	}
}

func BenchmarkBtreeMax(b *testing.B) {
	b.StopTimer()
	t := btree.New(4)
	for i := 0; i < 1e5; i++ {
		t.ReplaceOrInsert(intItem(i))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		m := t.Max()
		_ = m
	}
}

func BenchmarkBtreeDelete(b *testing.B) {
	b.StopTimer()
	t := btree.New(4)
	for i := 0; i < b.N; i++ {
		t.ReplaceOrInsert(intItem(b.N - i))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		t.Delete(intItem(i))
	}
}

func BenchmarkBtreeDeleteMin(b *testing.B) {
	b.StopTimer()
	t := btree.New(4)
	for i := 0; i < b.N; i++ {
		t.ReplaceOrInsert(intItem(b.N - i))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		t.DeleteMin()
	}
}
