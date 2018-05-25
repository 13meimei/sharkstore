package proto

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestHBContextCodec(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	uniqueIDS := make(map[uint64]struct{})
	for len(uniqueIDS) < 10000 {
		uniqueIDS[rnd.Uint64()%50000] = struct{}{}
	}
	var ctx HeartbeatContext
	for id := range uniqueIDS {
		ctx = append(ctx, id)
	}
	buf := EncodeHBConext(ctx)
	t.Logf("encoded length: %v", len(buf))
	actualCtx := DecodeHBContext(buf)
	if len(actualCtx) != len(ctx) {
		t.Fatalf("unexpected length: %v, expected: %v", len(actualCtx), len(ctx))
	}
	for _, id := range actualCtx {
		if _, ok := uniqueIDS[id]; !ok {
			t.Fatalf("wrong id: %d", id)
		}
	}

	sort.Slice(actualCtx, func(i, j int) bool {
		return actualCtx[i] < actualCtx[j]
	})

	if !reflect.DeepEqual(ctx, actualCtx) {
		t.Fatalf("wrong decoded ctx. expected: %v, actual: %v", ctx, actualCtx)
	}
}
