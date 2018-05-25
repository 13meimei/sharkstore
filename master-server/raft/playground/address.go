package main

import "fmt"

type address struct {
	heartbeat string
	replicate string
	client    string
}

var addrDatabase = make(map[uint64]*address)

func init() {
	for i := 1; i <= 9; i++ {
		addrDatabase[uint64(i)] = &address{
			heartbeat: fmt.Sprintf(":99%d1", i),
			replicate: fmt.Sprintf(":99%d2", i),
			client:    fmt.Sprintf(":99%d3", i),
		}
	}
}
