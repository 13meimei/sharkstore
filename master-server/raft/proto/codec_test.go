// Copyright 2018 The TigLabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
