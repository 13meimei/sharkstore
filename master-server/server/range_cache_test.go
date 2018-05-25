package server

import (
	"testing"
	"model/pkg/metapb"
	"bytes"
	"github.com/google/btree"
	"util/log"
	"fmt"
)

func TestBTreeUpdateAndSearsh(t *testing.T) {
	tree := newRangeTree()
	tree.update(&metapb.Range{StartKey: []byte("a"), EndKey: []byte("e")})
	tree.update(&metapb.Range{StartKey: []byte("e"), EndKey: []byte("k")})
	tree.update(&metapb.Range{StartKey: []byte("k"), EndKey: []byte("t")})
	tree.update(&metapb.Range{StartKey: []byte("t"), EndKey: []byte("w")})
	tree.update(&metapb.Range{StartKey: []byte("w"), EndKey: []byte("z")})

	r := tree.search([]byte("a"))
	if r == nil {
		t.Errorf("test failed")
		return
	}
	if bytes.Compare([]byte("a"), r.StartKey) != 0 || bytes.Compare([]byte("e"), r.EndKey) != 0 {
		t.Errorf("test failed")
		return
	}

	r = tree.search([]byte("u"))
	if r == nil {
		t.Errorf("test failed")
		return
	}
	if bytes.Compare([]byte("t"), r.StartKey) != 0 || bytes.Compare([]byte("w"), r.EndKey) != 0 {
		t.Errorf("test failed")
		return
	}
	tree.update(&metapb.Range{StartKey: []byte("a"), EndKey: []byte("b")})
	r = tree.search([]byte("a"))
	if r == nil {
		t.Errorf("test failed")
		return
	}
	if bytes.Compare([]byte("a"), r.StartKey) != 0 || bytes.Compare([]byte("b"), r.EndKey) != 0 {
		t.Errorf("test failed")
		return
	}
	r = tree.search([]byte("c"))
	if r != nil {
		t.Errorf("test failed")
		return
	}
}

func TestBTreeUpdateAndSearsh1(t *testing.T) {
	tree := newRangeTree()
	tree.update(&metapb.Range{StartKey: []byte("a"), EndKey: []byte("e")})
	tree.update(&metapb.Range{StartKey: []byte("e"), EndKey: []byte("k")})
	tree.update(&metapb.Range{StartKey: []byte("k"), EndKey: []byte("t")})
	tree.update(&metapb.Range{StartKey: []byte("t"), EndKey: []byte("w")})
	tree.update(&metapb.Range{StartKey: []byte("w"), EndKey: []byte("z")})

	tree.update(&metapb.Range{StartKey: []byte("l"), EndKey: []byte("q")})
	r := tree.search([]byte("p"))
	if r == nil {
		t.Errorf("test failed")
		return
	}
	if bytes.Compare([]byte("l"), r.StartKey) != 0 || bytes.Compare([]byte("q"), r.EndKey) != 0 {
		t.Errorf("test failed")
		return
	}
	r = tree.search([]byte("r"))
	if r != nil {
		t.Errorf("test failed")
		return
	}
}

func TestBTreeMultipleSearch(t *testing.T) {
	tree := newRangeTree()
	tree.update(&metapb.Range{StartKey: []byte("a"), EndKey: []byte("e")})
	tree.update(&metapb.Range{StartKey: []byte("e"), EndKey: []byte("k")})
	tree.update(&metapb.Range{StartKey: []byte("k"), EndKey: []byte("t")})
	tree.update(&metapb.Range{StartKey: []byte("t"), EndKey: []byte("w")})
	tree.update(&metapb.Range{StartKey: []byte("w"), EndKey: []byte("z")})

	rs := tree.multipleSearch([]byte("a"), 10)
	if len(rs) != 5 {
		t.Errorf("test failed %v", rs)
		return
	}
	r := rs[0]
	if bytes.Compare([]byte("a"), r.StartKey) != 0 || bytes.Compare([]byte("e"), r.EndKey) != 0 {
		t.Errorf("test failed")
		return
	}
}

func TestBTreeMultipleSearch1(t *testing.T) {
	tree := newRangeTree()
	tree.update(&metapb.Range{StartKey: []byte("a"), EndKey: []byte("e")})
	tree.update(&metapb.Range{StartKey: []byte("e"), EndKey: []byte("k")})
	tree.update(&metapb.Range{StartKey: []byte("k"), EndKey: []byte("t")})
	tree.update(&metapb.Range{StartKey: []byte("t"), EndKey: []byte("w")})
	tree.update(&metapb.Range{StartKey: []byte("w"), EndKey: []byte("z")})

	rs := tree.multipleSearch([]byte("f"), 10)
	if len(rs) != 4 {
		t.Errorf("test failed %v", rs)
		return
	}
	r := rs[0]
	if bytes.Compare([]byte("e"), r.StartKey) != 0 || bytes.Compare([]byte("k"), r.EndKey) != 0 {
		t.Errorf("test failed")
		return
	}
}

func TestBTreeMultipleSearch2(t *testing.T) {
	tree := newRangeTree()
	tree.update(&metapb.Range{StartKey: []byte("a"), EndKey: []byte("e")}) //97, 101
	//tree.update(&metapb.Range{StartKey: []byte("e"), EndKey: []byte("k")}) //101,107
	tree.update(&metapb.Range{StartKey: []byte("k"), EndKey: []byte("t")})  //107,116
	//tree.update(&metapb.Range{StartKey: []byte("t"), EndKey: []byte("w")}) //116, 119
	tree.update(&metapb.Range{StartKey: []byte("w"), EndKey: []byte("z")})  //119,112


	t.Logf("================")

	var ranges []*metapb.Range
	var startKeyTopy, endKeyTopy []byte

	var rngBefore *metapb.Range
	tree.tree.Descend(func(i btree.Item) bool {
		rng := i.(*rangeItem).region
		if startKeyTopy == nil {
			startKeyTopy = rng.StartKey
		}
		endKeyTopy = rng.EndKey
		if rngBefore != nil {
			if bytes.Compare(rngBefore.EndKey, rng.StartKey) < 0 {
				ranges = append(ranges, &metapb.Range{StartKey: rngBefore.EndKey, EndKey: rng.StartKey})
			}else if bytes.Compare(rngBefore.EndKey, rng.StartKey) > 0{
				log.Error("must bug!  topology scope[%s] and [%s] exists overlap", rngBefore.EndKey, rng.StartKey)
			}
		}
		rngBefore = rng
		return true
	})

	for _, r := range ranges{
		t.Logf("%v", r)
	}
	t.Logf("topoloy start key: %s, end key: %s", startKeyTopy, endKeyTopy)
}

func TestRangeDuplCheck(t *testing.T) {
	valueMap := make(map[uint64]*metapb.Range)
	valueMap[uint64(1)] = &metapb.Range{StartKey:[]byte("a"), EndKey:[]byte("b"), Id: uint64(1)}
	valueMap[uint64(2)] = &metapb.Range{StartKey:[]byte("a"), EndKey:[]byte("b"), Id: uint64(2)}
	valueMap[uint64(3)] = &metapb.Range{StartKey:[]byte("b"), EndKey:[]byte("c"), Id: uint64(3)}
	valueMap[uint64(4)] = &metapb.Range{StartKey:[]byte("d"), EndKey:[]byte("e"), Id: uint64(4)}


	var ranges []*metapb.Range
	rangeDuplMap := make(map[string][]uint64)
	jointFunc := func(startKey, endKey []byte) string {
		return fmt.Sprintf("%s%s", startKey, endKey)
	}
	var rangeIdSlice []uint64
	for _, r := range valueMap {
		key := jointFunc(r.StartKey, r.EndKey)
		rangeIdSlice = rangeDuplMap[key]
		rangeIdSlice = append(rangeIdSlice, r.GetId())
		rangeDuplMap[key] = rangeIdSlice
	}

	for _, slice := range rangeDuplMap {
		if len(slice) > 1 {
			for _, rngId := range slice {
				rng := valueMap[rngId]
				ranges = append(ranges, rng)
			}
		}
	}

	for _, res := range ranges {
		t.Logf("====%v", res)
	}

}