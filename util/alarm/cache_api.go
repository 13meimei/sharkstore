package alarm

import (
	"errors"
	"time"
	"github.com/gomodule/redigo/redis"
)

func (s *Server) jimDial() (redis.Conn, error) {
	if (len(s.jimUrl) == 0 || len(s.jimApAddr) == 0) {
		return nil, errors.New("no jim url or ap addr")
	}

	var dialOpts []redis.DialOption
	dialOpts = append(dialOpts, redis.DialPassword(s.jimUrl))

	if s.jimConnTimeoutSec > 0 {
		dialOpts = append(dialOpts, redis.DialConnectTimeout(s.jimConnTimeoutSec*time.Second))
	}
	if s.jimWriteTimeoutSec > 0 {
		dialOpts = append(dialOpts, redis.DialWriteTimeout(s.jimWriteTimeoutSec * time.Second))
	}
	if s.jimReadTimeoutSec > 0 {
		dialOpts = append(dialOpts, redis.DialReadTimeout(s.jimReadTimeoutSec*time.Second))
	}

	return redis.Dial("tcp", s.jimApAddr, dialOpts...)
}

func (s *Server) jimSendCommand(commandName string, args ...interface{}) (interface{}, error) {
	conn := s.jimClientPool.Get()
	return conn.Do(commandName, args...)
}
