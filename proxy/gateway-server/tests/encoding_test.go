package tests

import (
	"bytes"
	"math/rand"
	"testing"
	"time"
	"fmt"
	"util/encoding"
)

type ValueType int

const (
	TypeInt = iota
	TypeString
	TypeFloat
)

type Value struct {
	t ValueType
	v interface{}
}

// 随机组合几个不同类型的主键列，测试编码后排序性质是否保持
func TestEncodePK(t *testing.T) {
	for j := 0; j < 10000; j++ {
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		combineLen := rnd.Int()%10 + 1
		t.Logf("combine length: %v", combineLen)
		var values1, values2 []Value
		var encbuf1, encbuf2 []byte
		var c1, c2 int
		for i := 0; i < combineLen; i++ {
			v1, v2 := randTwoValue(rnd)
			values1 = append(values1, v1)
			values2 = append(values2, v2)
			encbuf1 = encodeValue(encbuf1, v1)
			encbuf2 = encodeValue(encbuf2, v2)
			if c1 == 0 {
				c1 = compareValue(t, v1, v2)
			}
		}
		// 比较编码后的buffer
		c2 = bytes.Compare(encbuf1, encbuf2)
		if c1 != c2 {
			t.Fatal("compare failed")
		}
	}
}

func compareValue(t *testing.T, lh, rh Value) int {
	if lh.t != rh.t {
		t.Fatalf("inconsistent value type when compare. left: %v, right: %v", lh, rh)
	}
	switch lh.t {
	case TypeInt:
		a := lh.v.(int64)
		b := rh.v.(int64)
		if a > b {
			return 1
		} else if a < b {
			return -1
		} else {
			return 0
		}
	case TypeString:
		a := lh.v.(string)
		b := rh.v.(string)
		if a > b {
			return 1
		} else if a < b {
			return -1
		} else {
			return 0
		}
	case TypeFloat:
		a := lh.v.(float64)
		b := rh.v.(float64)
		if a > b {
			return 1
		} else if a < b {
			return -1
		} else {
			return 0
		}
	default:
		t.Fatalf("unknown value type when compare")
		return 0
	}
}

func randTwoValue(rnd *rand.Rand) (Value, Value) {
	rt := rnd.Int() % 3
	switch rt {
	case 0: //int
		return Value{t: TypeInt, v: rnd.Int63()}, Value{t: TypeInt, v: rnd.Int63()}
	case 1: //float
		return Value{t: TypeFloat, v: rnd.Float64()}, Value{t: TypeFloat, v: rnd.Float64()}
	default: //string
		b1 := make([]byte, rnd.Int()%10+10)
		b2 := make([]byte, rnd.Int()%10+10)
		for i := 0; i < len(b1); i++ {
			b1[i] = byte(rnd.Int() % 255)
		}
		for i := 0; i < len(b2); i++ {
			b2[i] = byte(rnd.Int() % 255)
		}
		return Value{t: TypeString, v: string(b1)}, Value{t: TypeString, v: string(b2)}
	}
}

func encodeValue(buf []byte, v Value) []byte {
	switch v.t {
	case TypeInt:
		return encoding.EncodeVarintAscending(buf, v.v.(int64))
	case TypeFloat:
		return encoding.EncodeFloatAscending(buf, v.v.(float64))
	case TypeString:
		return encoding.EncodeStringAscending(buf, v.v.(string))
	default:
		panic("unknown value type when encode")
	}
}

func TestPrekey(t *testing.T){
	for i:=97;i<123;i++ {
		for j:=97;j<123;j++ {
			for k:=97;k<100;k++ {
				fmt.Printf("\"%c%c%c\",",i,j,k)
			}
		}
	}
	fmt.Println("***************")
	for i:=97;i<123;i++ {
		for j:=97;j<123;j++ {
			for k:=97;k<100;k++ {
				fmt.Printf("%c%c%c,",i,j,k)
			}
		}
	}
}

