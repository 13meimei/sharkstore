// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	//"runtime"
	"sync"

	"proxy/gateway-server/mysql"
	"util/hack"
	"util/log"
)

//client <-> proxy
type ClientConn struct {
	sync.Mutex

	pkg *mysql.PacketIO

	c net.Conn

	server *Server

	capability uint32

	connectionId uint32

	status    uint16
	collation mysql.CollationId
	charset   string

	user string
	db   string

	salt []byte

	closed bool

	lastInsertId int64
	affectedRows int64

	stmtId uint32

	stmts map[uint32]*Stmt //prepare相关,client端到proxy的stmt
}

var DEFAULT_CAPABILITY uint32 = mysql.CLIENT_LONG_PASSWORD | mysql.CLIENT_LONG_FLAG |
	mysql.CLIENT_CONNECT_WITH_DB | mysql.CLIENT_PROTOCOL_41 |
	mysql.CLIENT_TRANSACTIONS | mysql.CLIENT_SECURE_CONNECTION

var baseConnId uint32 = 10000

func (c *ClientConn) IsAllowConnect() bool {
	return true
}

func (c *ClientConn) Handshake() error {
	if err := c.writeInitialHandshake(); err != nil {
		log.Info("server Handshake %v send initial handshake err: %v",c.connectionId,  err.Error())
		return err
	}

	if err := c.readHandshakeResponse(); err != nil {
		//
		log.Info("server readHandshakeResponse %v  msg:read Handshake Response err: %v ",
			c.connectionId,err.Error())
		return err
	}

	if err := c.writeOK(nil); err != nil {
		log.Error("server readHandshakeResponse write ok fail %v err:%v",
			c.connectionId, err.Error())
		return err
	}

	c.pkg.Sequence = 0
	return nil
}

func (c *ClientConn) Close() error {
	if c.closed {
		return nil
	}

	c.c.Close()

	c.closed = true

	return nil
}

func (c *ClientConn) writeInitialHandshake() error {
	data := make([]byte, 4, 128)

	//min version 10
	data = append(data, 10)

	//server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)

	//connection id
	data = append(data, byte(c.connectionId), byte(c.connectionId>>8), byte(c.connectionId>>16), byte(c.connectionId>>24))

	//auth-plugin-data-part-1
	data = append(data, c.salt[0:8]...)

	//filter [00]
	data = append(data, 0)

	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY), byte(DEFAULT_CAPABILITY>>8))

	//charset, utf-8 default
	data = append(data, uint8(mysql.DEFAULT_COLLATION_ID))

	//status
	data = append(data, byte(c.status), byte(c.status>>8))

	//below 13 byte may not be used
	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY>>16), byte(DEFAULT_CAPABILITY>>24))

	//filter [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)

	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	//auth-plugin-data-part-2
	data = append(data, c.salt[8:]...)

	//filter [00]
	data = append(data, 0)

	return c.writePacket(data)
}

func (c *ClientConn) readPacket() ([]byte, error) {
	return c.pkg.ReadPacket()
}

func (c *ClientConn) writePacket(data []byte) error {
	return c.pkg.WritePacket(data)
}

func (c *ClientConn) writePacketBatch(total, data []byte, direct bool) ([]byte, error) {
	return c.pkg.WritePacketBatch(total, data, direct)
}

func (c *ClientConn) readHandshakeResponse() error {
	data, err := c.readPacket()

	if err != nil {
		return err
	}

	pos := 0

	//capability
	c.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4

	//skip max packet size
	pos += 4

	//charset, skip, if you want to use another charset, use set names
	//c.collation = CollationId(data[pos])
	pos++

	//skip reserved 23[00]
	pos += 23

	//user name
	c.user = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])

	pos += len(c.user) + 1

	//auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]

	checkAuth := mysql.CalcPassword(c.salt, []byte(c.server.cfg.Password))
	if c.user != c.server.cfg.User || !bytes.Equal(auth, checkAuth) {
		log.Error("ClientConn", "readHandshakeResponse", "error", 0,
			"auth", auth,
			"checkAuth", checkAuth,
			"client_user", c.user,
			"config_set_user", c.server.cfg.User,
			"passworld", c.server.cfg.Password)
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, c.user, c.c.RemoteAddr().String(), "Yes")
	}

	pos += authLen

	var db string
	if c.capability&mysql.CLIENT_CONNECT_WITH_DB > 0 {
		if len(data[pos:]) == 0 {
			return nil
		}

		db = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
		pos += len(c.db) + 1

	}
	c.db = db

	return nil
}

func (c *ClientConn) Run() {
	defer func() {
		if e := recover(); e != nil {
			log.Error("conn err, [%v]", e)
			//if err, ok := e.(error); ok {
			//	const size = 4096
			//	buf := make([]byte, size)
			//	buf = buf[:runtime.Stack(buf, false)]
			//
			//	log.Error("ClientConn", "Run",
			//		err.Error(), 0,
			//		"stack", string(buf))
			//}
		}
		c.Close()
	}()

	for {
		data, err := c.readPacket()

		if err != nil {
			log.Error("read packet err:%s, remoteIp is: %v", err.Error(), c.c.RemoteAddr().String())
			return
		}
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("read packet success:%s, remoteIp is: %v", hack.String(data), c.c.RemoteAddr().String())
		}

		if err := c.dispatch(data); err != nil {
			log.Error("server run error(%v), connetionId=%v, remoteIp is: %v", err.Error(), c.connectionId, c.c.RemoteAddr().String())
			c.writeError(err)
			if err == mysql.ErrBadConn {
				c.Close()
			}
		}

		if c.closed {
			return
		}

		c.pkg.Sequence = 0
	}
}

func (c *ClientConn) dispatch(data []byte) error {
	cmd := data[0]
	data = data[1:]

	switch cmd {
	case mysql.COM_QUIT:
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("quit")
		}
		c.handleRollback()
		c.Close()
		return nil
	case mysql.COM_QUERY:
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("COM_QUERY %s:", hack.String(data))
		}
		// For issue 1989
		// Input payload may end with byte '\0', we didn't find related mysql document about it, but mysql
		// implementation accept that case. So trim the last '\0' here as if the payload an EOF string.
		// See http://dev.mysql.com/doc/internals/en/com-query.html
		if len(data) > 0 && data[len(data)-1] == 0 {
			data = data[:len(data)-1]
		}
		return c.handleQuery(hack.String(data))
	case mysql.COM_PING:
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("ping")
		}
		return c.writeOK(nil)
	case mysql.COM_INIT_DB:
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("COM_INIT_DB %s:", hack.String(data))
		}
		return c.handleUseDB(hack.String(data))
	case mysql.COM_FIELD_LIST:
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("COM_FIELD_LIST %s:", hack.String(data))
		}
		return c.handleFieldList(data)
	case mysql.COM_STMT_PREPARE:
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("COM_STMT_PREPARE %s:", hack.String(data))
		}
		return c.handleStmtPrepare(hack.String(data))
	case mysql.COM_STMT_EXECUTE:
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("COM_STMT_EXECUTE %s:", hack.String(data))
		}
		return c.handleStmtExecute(data)
	case mysql.COM_STMT_CLOSE:
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("COM_STMT_CLOSE %s:", hack.String(data))
		}
		return c.handleStmtClose(data)
	case mysql.COM_STMT_SEND_LONG_DATA:
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("COM_STMT_CLOSE %s:", hack.String(data))
		}
		return c.handleStmtSendLongData(data)
	case mysql.COM_STMT_RESET:
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("COM_STMT_RESET %s:", hack.String(data))
		}
		return c.handleStmtReset(data)
	case mysql.COM_SET_OPTION:
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("COM_SET_OPTION %s:", hack.String(data))
		}
		return c.writeEOF(0)
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		log.Error("ClientConn", "dispatch", msg, 0)
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}

	return nil
}

func (c *ClientConn) writeOK(r *mysql.Result) error {
	if r == nil {
		r = &mysql.Result{Status: c.status}
	}
	data := make([]byte, 4, 32)

	data = append(data, mysql.OK_HEADER)

	data = append(data, mysql.PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, mysql.PutLengthEncodedInt(r.InsertId)...)

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, 0, 0)
	}

	return c.writePacket(data)
}

func (c *ClientConn) writeError(e error) error {
	var m *mysql.SqlError
	var ok bool
	if m, ok = e.(*mysql.SqlError); !ok {
		m = mysql.NewError(mysql.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))

	data = append(data, mysql.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return c.writePacket(data)
}

func (c *ClientConn) writeEOF(status uint16) error {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return c.writePacket(data)
}

func (c *ClientConn) writeEOFBatch(total []byte, status uint16, direct bool) ([]byte, error) {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return c.writePacketBatch(total, data, direct)
}
