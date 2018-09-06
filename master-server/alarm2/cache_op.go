package alarm2

import (
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
	"strings"
)

func (s *Server) newJimClient() *redis.Pool {
	return &redis.Pool{
		MaxIdle: 3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return s.jimDial()
		},
	}
}

func (s *Server) jimDial() (redis.Conn, error) {
	c := s.conf

	if len(c.JimUrl) == 0 || len(c.JimApAddr) == 0 {
		return nil, errors.New("no jim url or ap addr")
	}

	var dialOpts []redis.DialOption
	dialOpts = append(dialOpts, redis.DialPassword(c.JimUrl))

	if c.JimConnTimeoutSec > 0 {
		dialOpts = append(dialOpts, redis.DialConnectTimeout(c.JimConnTimeoutSec*time.Second))
	}
	if c.JimWriteTimeoutSec > 0 {
		dialOpts = append(dialOpts, redis.DialWriteTimeout(c.JimWriteTimeoutSec * time.Second))
	}
	if c.JimReadTimeoutSec > 0 {
		dialOpts = append(dialOpts, redis.DialReadTimeout(c.JimReadTimeoutSec*time.Second))
	}

	return redis.Dial("tcp", c.JimApAddr, dialOpts...)
}

func (s *Server) jimCommand(commandName string, args ...interface{}) (interface{}, error) {
	conn := s.jimClient.Get()
	return conn.Do(commandName, args...)
}

func (s *Server) jimGetCommand(key string) error {

	reply, err := s.jimCommand("get", key)
	if err != nil {
		return err
	}
	if _, err := redis.String(reply, err); err != nil {
		return err
	}
	return nil
}

func (s *Server) jimSetexCommand(key, value string, expireTime int64) error {
	reply, err := s.jimCommand("setex", key, expireTime, value)
	if err != nil {
		return err
	}
	replyStr, err := redis.String(reply, err)
	if err != nil {
		return err
	}
	if strings.Compare(strings.ToLower(replyStr), "ok") != 0 {
		return err
	}
	return nil
}
