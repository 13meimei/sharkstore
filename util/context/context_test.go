package context

import (
	"testing"
	"time"
)

func TestTimerContext(t *testing.T) {
	ctx := NewTimerCtx(time.Second)
	select {
	case <-ctx.Done():
	default:
	}
	time.Sleep(time.Millisecond * 500)
	select {
	case <-ctx.Done():
	default:
	}
	time.Sleep(time.Second)
	select {
	case <-ctx.Done():
	case <-time.After(time.Millisecond * 100):
		t.Error("test failed")
		return
	}
	select {
	case <-ctx.Done():
	default:
		t.Error("test failed")
		return
	}
	t.Log("ctx reset")
	ctx.Reset(time.Second)
	time.Sleep(time.Second)
	select {
	case <-ctx.Done():
	case <-time.After(time.Millisecond * 100):
		t.Error("test failed")
		return
	}
	select {
	case <-ctx.Done():
	default:
		t.Error("test failed")
		return
	}
	t.Log("test success!!!")
}

func TestTimerContextTimeout(t *testing.T) {
	ctx := NewTimerCtx(time.Second)
	select {
	case <-ctx.Done():
	default:
	}
	time.Sleep(time.Second)
	ctx.Reset(time.Second)
	select {
	case <-ctx.Done():
		t.Error("test failed")
		return
	default:
	}
	time.Sleep(time.Millisecond * 500)
	select {
	case <-ctx.Done():
		t.Error("test failed")
		return
	default:
	}
	time.Sleep(time.Second)
	select {
	case <-ctx.Done():
	default:
		t.Error("test failed")
		return
	}
	t.Log("test success!!!")
}