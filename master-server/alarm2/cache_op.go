package alarm2

import (
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
	"strings"
	"fmt"
	"encoding/json"
)

type cacheOp interface {
	get(key string) (string, error)
	setex(key, value string, expireTime int64) error
	exists(key string) error
}

type cacheOpImpl struct {
	server *Server
}

func (s *Server) newCacheOpImpl() *cacheOpImpl {
	return &cacheOpImpl{
		server: s,
	}
}

func (opImpl *cacheOpImpl) get(key string) (string, error) {
	return opImpl.server.jimGetCommand(key)
}

func (opImpl *cacheOpImpl) setex(key, value string, expireTime int64) error {
	return opImpl.server.jimSetexCommand(key, value, expireTime)
}

func (opImp *cacheOpImpl) exists(key string) error {
	return opImp.server.jimExistsCommand(key)
}

func encodeCacheKey(key cacheKey) (keyStr string, err error) {
	name := strings.ToLower(key.AppName)
	switch {
	case strings.HasPrefix(name, APPNAME_GATEWAY):
		keyStr = fmt.Sprintf("%s%s%v%s%v%s%v", key.RuleName,
			APPNAME_JOIN_LETTER, APPNAME_GATEWAY,
			APPNAME_JOIN_LETTER, key.ClusterId,
			APPNAME_JOIN_LETTER, key.AppAddr)
	case strings.HasPrefix(name, APPNAME_MASTER):
		keyStr = fmt.Sprintf("%s%s%v%s%v%s%v", key.RuleName,
			APPNAME_JOIN_LETTER, APPNAME_MASTER,
			APPNAME_JOIN_LETTER, key.ClusterId,
			APPNAME_JOIN_LETTER, key.AppAddr)
	case strings.HasPrefix(name, APPNAME_METRIC):
		keyStr = fmt.Sprintf("%s%s%v%s%v%s%v", key.RuleName,
			APPNAME_JOIN_LETTER, APPNAME_METRIC,
			APPNAME_JOIN_LETTER, key.ClusterId,
			APPNAME_JOIN_LETTER, key.AppAddr)
	default:
		err = errors.New("unknown app name")
	}
	return
}

func decodeCacheKey(keyStr string) (key cacheKey, err error) {
	return
}

func encodeCacheValue(value cacheValue) (string, error) {
	ret, err := json.Marshal(value)
	return string(ret), err
}

func decodeCacheValue(valueStr string) (value cacheValue, err error) {
	err = json.Unmarshal([]byte(valueStr), &value)
	return
}

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

func (s *Server) jimGetCommand(key string) (string, error) {
	reply, err := s.jimCommand("get", key)
	if err != nil {
		return "", err
	}
	replyStr, err := redis.String(reply, err)
	if err != nil {
		return "", err
	}
	return replyStr, nil
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
		return errors.New("reply string is not ok")
	}
	return nil
}

func (s *Server) jimExistsCommand(key string) error {
	reply, err := s.jimCommand("exists", key)
	if err != nil {
		return err
	}
	replyInt, err := redis.Int(reply, err)
	if err != nil {
		return err
	}
	if replyInt == 0 { // key not exists
		return errors.New("reply int is 0")
	}
	return nil
}
