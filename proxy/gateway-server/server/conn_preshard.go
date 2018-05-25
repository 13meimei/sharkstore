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
	//"fmt"
	//"strings"

	"proxy/gateway-server/errors"
	"proxy/gateway-server/mysql"
)

type ExecuteDB struct {
	
	sql      string
}

func (c *ClientConn) isBlacklistSql(sql string) bool {
	
	return false
}

//preprocessing sql before parse sql
func (c *ClientConn) preHandleShard(sql string) (bool, error) {
	var rs []*mysql.Result
	var err error
	var executeDB *ExecuteDB

	if len(sql) == 0 {
		return false, errors.ErrCmdUnsupport
	}
	
	if err != nil {
		//this SQL doesn't need execute in the backend.
		if err == errors.ErrIgnoreSQL {
			err = c.writeOK(nil)
			if err != nil {
				return false, err
			}
			return true, nil
		}
		return false, err
	}
	//need shard sql
	if executeDB == nil {
		return false, nil
	}

	//execute.sql may be rewritten in getShowExecDB
	//rs, err = c.executeInNode(conn, executeDB.sql, nil)
	if err != nil {
		return false, err
	}

	

	c.lastInsertId = int64(rs[0].InsertId)
	c.affectedRows = int64(rs[0].AffectedRows)

	if rs[0].Resultset != nil {
		err = c.writeResultset(c.status, rs[0].Resultset)
	} else {
		err = c.writeOK(rs[0])
	}

	if err != nil {
		return false, err
	}

	return true, nil
}


//handle show columns/fields
func (c *ClientConn) handleShowColumns(sql string, tokens []string,
	tokensLen int, executeDB *ExecuteDB) error {
//	var ruleDB string
//	for i := 0; i < tokensLen; i++ {
//		tokens[i] = strings.ToLower(tokens[i])
//		//handle SQL:
//		//SHOW [FULL] COLUMNS FROM tbl_name [FROM db_name] [like_or_where]
//		if (strings.ToLower(tokens[i]) == mysql.TK_STR_FIELDS ||
//			strings.ToLower(tokens[i]) == mysql.TK_STR_COLUMNS) &&
//			i+2 < tokensLen {
//			if strings.ToLower(tokens[i+1]) == mysql.TK_STR_FROM {
//				tableName := strings.Trim(tokens[i+2], "`")
//				//get the ruleDB
//				if i+4 < tokensLen && strings.ToLower(tokens[i+1]) == mysql.TK_STR_FROM {
//					ruleDB = strings.Trim(tokens[i+4], "`")
//				} else {
//					ruleDB = c.db
//				}
//				showRouter := c.schema.rule
//				showRule := showRouter.GetRule(ruleDB, tableName)
//				//this SHOW is sharding SQL
//				if showRule.Type != router.DefaultRuleType {
//					if 0 < len(showRule.SubTableIndexs) {
//						tableIndex := showRule.SubTableIndexs[0]
//						nodeIndex := showRule.TableToNode[tableIndex]
//						nodeName := showRule.Nodes[nodeIndex]
//						tokens[i+2] = fmt.Sprintf("%s_%04d", tableName, tableIndex)
//						executeDB.sql = strings.Join(tokens, " ")
//						executeDB.ExecNode = c.schema.nodes[nodeName]
//						return nil
//					}
//				}
//			}
//		}
//	}
	return nil
}

