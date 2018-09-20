package server

import (
	"fmt"

	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"

	"util/log"
)

// HandleUpdate handle update
func (p *Proxy) HandleUpdate(db string, stmt *sqlparser.Update, args []interface{}) (*mysql.Result, error) {
	var err error

	parser := &StmtParser{}

	// 解析表名
	tableName := parser.parseTable(stmt)
	t := p.router.FindTable(db, tableName)
	if t == nil {
		log.Error("[update] table %s.%s doesn.t exist", db, tableName)
		return nil, fmt.Errorf("Table '%s.%s' doesn't exist", db, tableName)
	}

	var exprs []UpdateField
	exprs, err = parser.parseUpdateExprs(stmt.Exprs)
	if err != nil {
		log.Error("[update] parse update exprs failed: %v", err)
		return nil, fmt.Errorf("parse update exprs failed: %v", err)
	}

	// 解析where条件
	var matchs []Match
	if stmt.Where != nil {
		// TODO: 支持OR表达式
		matchs, err = parser.parseWhere(stmt.Where)
		if err != nil {
			log.Error("[update] parse where error(%v)", err.Error())
			return nil, err
		}
	}

	var limit *Limit
	if stmt.Limit != nil {
		offset, count, err := parseLimit(stmt.Limit)
		if err != nil {
			log.Error("[update] parse limit error[%v]", err)
			return nil, err
		}
		if count > DefaultMaxRawCount {
			log.Warn("limit count exceeding the maximum limit")
			return nil, ErrExceedMaxLimit
		}
		limit = &Limit{offset: offset, rowCount: count}
	}

	if log.GetFileLogger().IsEnableDebug() {
		log.Debug("update exprs: %v", exprs)
		log.Debug("matchs: %v", matchs)
		log.Debug("limit: %v", limit)
	}

	affected, err := p.doUpdate(t, exprs, matchs, limit)
	if err != nil {
		return nil, err
	}

	res := new(mysql.Result)
	res.AffectedRows = affected
	return res, nil
}

func (p *Proxy) doUpdate(t *Table, exprs []UpdateField, matches []Match, limit *Limit) (affected uint64, err error) {
	pbMatches, err := makePBMatches(t, matches)
	if err != nil {
		log.Error("[update]covert filter failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
		return
	}

	pbLimit, err := makePBLimit(p, limit)
	if err != nil {
		log.Error("[update]covert limit failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
		return
	}

	// todo update

	return
}