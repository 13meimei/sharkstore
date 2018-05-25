package context

import (
	"sync"
)

var ctxPool *sync.Pool = &sync.Pool{
	New:func() interface {}{
		return &TimerCtx{}
	},
}

func GetContext() *TimerCtx {
	return ctxPool.Get().(*TimerCtx)
}

func PutContext(ctx *TimerCtx){
	if ctx == nil {
		return
	}
	ctxPool.Put(ctx)
}
