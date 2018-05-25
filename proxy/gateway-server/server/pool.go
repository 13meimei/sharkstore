package server

import "sync"

var selectTaskPool *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return &SelectTask{done: make(chan error, 1)}
	},
}

func GetSelectTask() *SelectTask {
	return selectTaskPool.Get().(*SelectTask)
}

func PutSelectTask(task *SelectTask) {
	if task == nil {
		return
	}
	task.Reset()
	selectTaskPool.Put(task)
}

var insertTaskPool *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return &InsertTask{done: make(chan error, 1)}
	},
}

func GetInsertTask() *InsertTask {
	return insertTaskPool.Get().(*InsertTask)
}

func PutInsertTask(task *InsertTask) {
	if task == nil {
		return
	}
	task.Reset()
	insertTaskPool.Put(task)
}
