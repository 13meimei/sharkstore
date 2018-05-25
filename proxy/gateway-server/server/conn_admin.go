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
	"strings"

	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"
	"util/log"
)

const (
	Master = "master"
	Slave  = "slave"

	ServerRegion = "server"
	NodeRegion   = "node"

	//op
	ADMIN_OPT_ADD     = "add"
	ADMIN_OPT_DEL     = "del"
	ADMIN_OPT_UP      = "up"
	ADMIN_OPT_DOWN    = "down"
	ADMIN_OPT_SHOW    = "show"
	ADMIN_OPT_CHANGE  = "change"
	ADMIN_SAVE_CONFIG = "save"

	ADMIN_PROXY         = "proxy"
	ADMIN_NODE          = "node"
	ADMIN_SCHEMA        = "schema"
	ADMIN_LOG_SQL       = "log_sql"
	ADMIN_SLOW_LOG_TIME = "slow_log_time"
	ADMIN_ALLOW_IP      = "allow_ip"
	ADMIN_BLACK_SQL     = "black_sql"

	ADMIN_CONFIG = "config"
	ADMIN_STATUS = "status"
)

var cmdServerOrder = []string{"opt", "k", "v"}
var cmdNodeOrder = []string{"opt", "node", "k", "v"}

func (c *ClientConn) handleNodeCmd(rows sqlparser.InsertRows) error {
	//	var err error
	//	var opt, nodeName, role, addr string
	//
	//	vals := rows.(sqlparser.Values)
	//	if len(vals) == 0 {
	//		return errors.ErrCmdUnsupport
	//	}
	//
	//	tuple := vals[0].(sqlparser.ValTuple)
	//	if len(tuple) != len(cmdNodeOrder) {
	//		return errors.ErrCmdUnsupport
	//	}
	//
	//	opt = sqlparser.String(tuple[0])
	//	opt = strings.Trim(opt, "'")
	//
	//	nodeName = sqlparser.String(tuple[1])
	//	nodeName = strings.Trim(nodeName, "'")
	//
	//	role = sqlparser.String(tuple[2])
	//	role = strings.Trim(role, "'")
	//
	//	addr = sqlparser.String(tuple[3])
	//	addr = strings.Trim(addr, "'")
	//
	//	switch strings.ToLower(opt) {
	//	case ADMIN_OPT_ADD:
	//		err = c.AddDatabase(
	//			nodeName,
	//			role,
	//			addr,
	//		)
	//	case ADMIN_OPT_DEL:
	//		err = c.DeleteDatabase(
	//			nodeName,
	//			role,
	//			addr,
	//		)
	//
	//	case ADMIN_OPT_UP:
	//		err = c.UpDatabase(
	//			nodeName,
	//			role,
	//			addr,
	//		)
	//	case ADMIN_OPT_DOWN:
	//		err = c.DownDatabase(
	//			nodeName,
	//			role,
	//			addr,
	//		)
	//	default:
	//		err = errors.ErrCmdUnsupport
	//		golog.Error("ClientConn", "handleNodeCmd", err.Error(),
	//			c.connectionId, "opt", opt)
	//	}
	return nil
}

func (c *ClientConn) handleServerCmd(rows sqlparser.InsertRows) (*mysql.Resultset, error) {
	var err error
	var result *mysql.Resultset
	//	var opt, k, v string
	//
	//	vals := rows.(sqlparser.Values)
	//	if len(vals) == 0 {
	//		return nil, errors.ErrCmdUnsupport
	//	}
	//
	//	tuple := vals[0].(sqlparser.ValTuple)
	//	if len(tuple) != len(cmdServerOrder) {
	//		return nil, errors.ErrCmdUnsupport
	//	}
	//
	//	opt = sqlparser.String(tuple[0])
	//	opt = strings.Trim(opt, "'")
	//
	//	k = sqlparser.String(tuple[1])
	//	k = strings.Trim(k, "'")
	//
	//	v = sqlparser.String(tuple[2])
	//	v = strings.Trim(v, "'")
	//
	//	switch strings.ToLower(opt) {
	//	case ADMIN_OPT_SHOW:
	//		result, err = c.handleAdminShow(k, v)
	//	case ADMIN_OPT_CHANGE:
	//		err = c.handleAdminChange(k, v)
	//	case ADMIN_OPT_ADD:
	//		err = c.handleAdminAdd(k, v)
	//	case ADMIN_OPT_DEL:
	//		err = c.handleAdminDelete(k, v)
	//	case ADMIN_SAVE_CONFIG:
	//		err = c.handleAdminSave(k, v)
	//	default:
	//		err = errors.ErrCmdUnsupport
	//		golog.Error("ClientConn", "handleNodeCmd", err.Error(),
	//			c.connectionId, "opt", opt)
	//	}
	//	if err != nil {
	//		return nil, err
	//	}

	return result, err
}

func (c *ClientConn) AddDatabase(nodeName string, role string, addr string) error {
	//can not add a new master database
	//	if role != Slave {
	//		return errors.ErrCmdUnsupport
	//	}
	//
	//	return c.proxy.AddSlave(nodeName, addr)
	return nil
}

func (c *ClientConn) DeleteDatabase(nodeName string, role string, addr string) error {
	//can not delete a master database
	//	if role != Slave {
	//		return errors.ErrCmdUnsupport
	//	}
	//
	//	return c.proxy.DeleteSlave(nodeName, addr)
	return nil
}

func (c *ClientConn) UpDatabase(nodeName string, role string, addr string) error {
	//	if role != Master && role != Slave {
	//		return errors.ErrCmdUnsupport
	//	}
	//	if role == Master {
	//		return c.proxy.UpMaster(nodeName, addr)
	//	}
	//
	//	return c.proxy.UpSlave(nodeName, addr)
	return nil
}

func (c *ClientConn) DownDatabase(nodeName string, role string, addr string) error {
	//	if role != Master && role != Slave {
	//		return errors.ErrCmdUnsupport
	//	}
	//	if role == Master {
	//		return c.proxy.DownMaster(nodeName, addr)
	//	}
	//
	//	return c.proxy.DownSlave(nodeName, addr)
	return nil
}

func (c *ClientConn) checkCmdOrder(region string, columns sqlparser.Columns) error {
	//	var cmdOrder []string
	//	node := sqlparser.SelectExprs(columns)
	//
	//	switch region {
	//	case NodeRegion:
	//		cmdOrder = cmdNodeOrder
	//	case ServerRegion:
	//		cmdOrder = cmdServerOrder
	//	default:
	//		return errors.ErrCmdUnsupport
	//	}
	//
	//	for i := 0; i < len(node); i++ {
	//		val := sqlparser.String(node[i])
	//		if val != cmdOrder[i] {
	//			return errors.ErrCmdUnsupport
	//		}
	//	}

	return nil
}

func (c *ClientConn) handleAdmin(admin *sqlparser.Admin) error {
	// TODO: check Privilege
	// if c.user != "admin" {
	// 	return
	// }

	cmd, args := parseAdminArgs(admin)
	res, err := c.server.proxy.HandleAdmin(c.db, cmd, args)
	if err != nil {
		log.Error("handle admin failed(%v), cmd: %s, args: %s", err, cmd, strings.Join(args, " "))
		return c.writeError(err)
	}

	return c.writeResultset(res.Status, res.Resultset)
}

func (c *ClientConn) handleShowProxyConfig() (*mysql.Resultset, error) {
	var names []string = []string{"Key", "Value"}
	var rows [][]string
	//	var nodeNames []string
	//
	const (
		Column = 2
	)
	//	for name := range c.schema.nodes {
	//		nodeNames = append(nodeNames, name)
	//	}
	//
	//	rows = append(rows, []string{"Addr", c.proxy.cfg.Addr})
	//	rows = append(rows, []string{"User", c.proxy.cfg.User})
	//	rows = append(rows, []string{"LogPath", c.proxy.cfg.LogPath})
	//	rows = append(rows, []string{"LogLevel", c.proxy.cfg.LogLevel})
	//	rows = append(rows, []string{"LogSql", c.proxy.logSql[c.proxy.logSqlIndex]})
	//	rows = append(rows, []string{"SlowLogTime", strconv.Itoa(c.proxy.slowLogTime[c.proxy.slowLogTimeIndex])})
	//	rows = append(rows, []string{"Nodes_Count", fmt.Sprintf("%d", len(c.proxy.nodes))})
	//	rows = append(rows, []string{"Nodes_List", strings.Join(nodeNames, ",")})
	//	rows = append(rows, []string{"ClientConns", fmt.Sprintf("%d", c.proxy.counter.ClientConns)})
	//	rows = append(rows, []string{"ClientQPS", fmt.Sprintf("%d", c.proxy.counter.OldClientQPS)})
	//	rows = append(rows, []string{"ErrLogTotal", fmt.Sprintf("%d", c.proxy.counter.OldErrLogTotal)})
	//	rows = append(rows, []string{"SlowLogTotal", fmt.Sprintf("%d", c.proxy.counter.OldSlowLogTotal)})

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	return c.buildResultset(nil, names, values)
}

func (c *ClientConn) handleShowNodeConfig() (*mysql.Resultset, error) {
	var names []string = []string{
		"Node",
		"Address",
		"Type",
		"State",
		"LastPing",
		"MaxConn",
		"IdleConn",
	}
	var rows [][]string
	const (
		Column = 7
	)
	//
	//	//var nodeRows [][]string
	//	for name, node := range c.schema.nodes {
	//		//"master"
	//		rows = append(
	//			rows,
	//			[]string{
	//				name,
	//				node.Master.Addr(),
	//				"master",
	//				node.Master.State(),
	//				fmt.Sprintf("%v", time.Unix(node.Master.GetLastPing(), 0)),
	//				strconv.Itoa(node.Cfg.MaxConnNum),
	//				strconv.Itoa(node.Master.IdleConnCount()),
	//			})
	//		//"slave"
	//		for _, slave := range node.Slave {
	//			if slave != nil {
	//				rows = append(
	//					rows,
	//					[]string{
	//						name,
	//						slave.Addr(),
	//						"slave",
	//						slave.State(),
	//						fmt.Sprintf("%v", time.Unix(slave.GetLastPing(), 0)),
	//						strconv.Itoa(node.Cfg.MaxConnNum),
	//						strconv.Itoa(slave.IdleConnCount()),
	//					})
	//			}
	//		}
	//	}
	//rows = append(rows, nodeRows...)
	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	return c.buildResultset(nil, names, values)
}

func (c *ClientConn) handleShowSchemaConfig() (*mysql.Resultset, error) {
	var Column = 7
	var rows [][]string
	var names []string = []string{
		"DB",
		"Table",
		"Type",
		"Key",
		"Nodes_List",
		"Locations",
		"TableRowLimit",
	}

	//default Rule
	//	var defaultRule = c.schema.rule.DefaultRule
	//	rows = append(
	//		rows,
	//		[]string{
	//			defaultRule.DB,
	//			defaultRule.Table,
	//			defaultRule.Type,
	//			defaultRule.Key,
	//			strings.Join(defaultRule.Nodes, ", "),
	//			"",
	//			"0",
	//		},
	//	)
	//
	//	schemaConfig := c.proxy.cfg.Schema
	//	shardRule := schemaConfig.ShardRule
	//
	//	for _, r := range shardRule {
	//		rows = append(
	//			rows,
	//			[]string{
	//				r.DB,
	//				r.Table,
	//				r.Type,
	//				r.Key,
	//				strings.Join(r.Nodes, ", "),
	//				hack.ArrayToString(r.Locations),
	//				strconv.Itoa(r.TableRowLimit),
	//			},
	//		)
	//	}

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	return c.buildResultset(nil, names, values)
}

func (c *ClientConn) handleShowAllowIPConfig() (*mysql.Resultset, error) {
	var Column = 1
	var rows [][]string
	var names []string = []string{
		"AllowIP",
	}

	//allow ips
	//	var allowips = c.proxy.allowips[c.proxy.allowipsIndex]
	//	if len(allowips) != 0 {
	//		for _, v := range allowips {
	//			if v == nil {
	//				continue
	//			}
	//			rows = append(rows,
	//				[]string{
	//					v.String(),
	//				})
	//		}
	//	}

	if len(rows) == 0 {
		rows = append(rows, []string{""})
	}

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	return c.buildResultset(nil, names, values)
}

func (c *ClientConn) handleShowProxyStatus() (*mysql.Resultset, error) {
	var Column = 1
	var rows [][]string
	var names []string = []string{
		"status",
	}

	var status string
	status = c.server.Status()
	rows = append(rows,
		[]string{
			status,
		})

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	return c.buildResultset(nil, names, values)
}

func (c *ClientConn) handleShowBlackSqlConfig() (*mysql.Resultset, error) {
	var Column = 1
	var rows [][]string
	var names []string = []string{
		"BlackListSql",
	}

	//black sql
	//	var blackListSqls = c.proxy.blacklistSqls[c.proxy.blacklistSqlsIndex].sqls
	//	if len(blackListSqls) != 0 {
	//		for _, v := range blackListSqls {
	//			rows = append(rows,
	//				[]string{
	//					v,
	//				})
	//		}
	//	}

	if len(rows) == 0 {
		rows = append(rows, []string{""})
	}

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	return c.buildResultset(nil, names, values)
}

func (c *ClientConn) handleChangeProxy(v string) error {
	return c.server.ChangeProxy(v)
}

func (c *ClientConn) handleChangeLogSql(v string) error {
	return nil //return c.proxy.ChangeLogSql(v)
}

func (c *ClientConn) handleChangeSlowLogTime(v string) error {
	return nil //return c.proxy.ChangeSlowLogTime(v)
}

func (c *ClientConn) handleAddAllowIP(v string) error {
	//	v = strings.TrimSpace(v)
	//	err := c.proxy.AddAllowIP(v)
	//	return err
	return nil //
}

func (c *ClientConn) handleDelAllowIP(v string) error {
	//	v = strings.TrimSpace(v)
	//	err := c.proxy.DelAllowIP(v)
	//	return err
	return nil
}

func (c *ClientConn) handleAddBlackSql(v string) error {
	//	v = strings.TrimSpace(v)
	//	err := c.proxy.AddBlackSql(v)
	//	return err
	return nil
}

func (c *ClientConn) handleDelBlackSql(v string) error {
	//	v = strings.TrimSpace(v)
	//	err := c.proxy.DelBlackSql(v)
	//	return err
	return nil
}
