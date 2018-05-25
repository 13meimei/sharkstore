package server

import (
	"net"
	"bufio"
	"time"
	"sync"
)

type Session struct {
	id uint64
	conn net.Conn
	closed int

	rw *bufio.ReadWriter

	timestamp time.Time

	wg sync.WaitGroup
}

func NewSession(id uint64, conn net.Conn) *Session {
	return nil
}

func (s *Session) ID() uint64 {
	return s.id
}

func (s *Session) IsClosed() bool {
	return false
}

func (s *Session) Close() {

}

func (s *Session) Process() {

}
