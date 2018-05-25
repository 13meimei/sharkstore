package util

import (
	"testing"
	"bytes"
	"fmt"
)

func TestEncodeStorePrefix(t *testing.T) {
	var prefixs [][]byte
	for i := 0; i < 0xffff; i++ {
		prefixs = append(prefixs, EncodeStorePrefix(Store_Prefix_KV, uint64(i)))
	}
	for i := 0; i < 0xffff; i++ {
		if i + 1 < 0xffff {
			if bytes.Compare(prefixs[i], prefixs[i + 1]) >= 0 {
				t.Error("test failed")
				return
			}
		}
	}
}

func TestKeyPairSore(t *testing.T)  {
	var kps KeyPairSlice
	kps = append(kps, KeyPair{[]byte("a"),[]byte("b")})
	kps = append(kps, KeyPair{[]byte("t"),[]byte("w")})
	kps = append(kps, KeyPair{[]byte("w"),[]byte("z")})
	kps = append(kps, KeyPair{[]byte("b"),[]byte("t")})

	kps.Sort()

	for _, r := range kps {
		t.Logf(fmt.Sprintln("%v", r))
	}
}
