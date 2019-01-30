package server

import (
	"fmt"
	"bytes"

	"util/log"
	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"
	"proxy/store/dskv"
	"pkg-go/ds_client"
	"model/pkg/kvrpcpb"
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

	var fieldList []*kvrpcpb.Field
	fieldList, err = parser.parseUpdateFields(t, stmt)
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
		if offset != 0 {
			log.Error("[update] unsupported limit offset")
			return nil, fmt.Errorf("parse update limit failed: unsupported limit offset")
		}
		//todo 是否需要加限制
		//if count > DefaultMaxRawCount {
		//	log.Warn("limit count exceeding the maximum limit")
		//	return nil, ErrExceedMaxLimit
		//}
		limit = &Limit{rowCount: count}
	}
	//先不支持
	//if stmt.OrderBy != nil {
	//
	//}

	if log.GetFileLogger().IsEnableDebug() {
		log.Debug("update exprs: %v, matchs: %v, limit: %v", fieldList, matchs, limit)
	}
	affected, err := p.doUpdate(t, fieldList, matchs, limit, nil)
	if err != nil {
		return nil, err
	}

	res := new(mysql.Result)
	res.AffectedRows = affected
	return res, nil
}

func (p *Proxy) doUpdate(t *Table, exprs []*kvrpcpb.Field, matches []Match, limit *Limit, userScope *Scope) (affected uint64, err error) {
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

	var key []byte
	var scope *kvrpcpb.Scope
	if userScope != nil {
		scope = &kvrpcpb.Scope{
			Start: userScope.Start,
			Limit: userScope.End,
		}
	} else {
		key, scope, err = findPKScope(t, pbMatches)
		if err != nil {
			log.Error("[update]get pk scope failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
			return
		}
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("[update]pk key: [%v], scope: %v", key, scope)
		}
	}

	//检查更新的字段是否包含需要更新的index字段
	needSelectFields, indexMap := getSelectFieldsForIndex(t, exprs)
	if len(needSelectFields) > 0 {
		//retrieve: pk value and index field old value
		sreq := &kvrpcpb.SelectRequest{
			Key:          key,
			Scope:        scope,
			FieldList:    needSelectFields,
			WhereFilters: pbMatches,
			Limit:        pbLimit,
		}
		//pk1,...,pkn,index1(old),...,indexn(old), correspond to needSelectFields
		var data [][]*Row
		data, err = p.selectRemote(t, sreq)
		if err != nil {
			return
		}

		var (
			oldIndexKeys    [][]byte
			newIndexKvPairs []*kvrpcpb.KeyValue
			start           = len(t.PKS())
			end             = len(needSelectFields)
		)

		for _, rowData := range data {
			if len(rowData) != len(needSelectFields) {
				err = fmt.Errorf("select error")
				return
			}

			//index
			for i := start; i < end; i++ {
				var (
					oldKey []byte
					newKey, newValue []byte
				)
				//todo
				col := needSelectFields[i].Column
				//oldKey, err = encodeUniqueIndexKey(t, col, rowData[i].fields[0].value)
				//if err != nil {
				//	return
				//}
				newKey, err = encodeUniqueIndexKey(t, col, indexMap[col.Name].Value)
				if err != nil {
					return
				}

				//if col.Unique {
				//	oldKey, err = encodePrimaryKeys(t, key, colMap, rowValue)
				//	if err != nil {
				//		return
				//	}
				//	newKey, err = encodePrimaryKeys(t, key, colMap, rowValue)
				//	if err != nil {
				//		return
				//	}
				//} else {
				//
				//
				//}
				oldIndexKeys = append(oldIndexKeys, oldKey)
				newIndexKvPairs = append(newIndexKvPairs, &kvrpcpb.KeyValue{
					Key: newKey,
					Value: newValue,
				})
			}
		}
		context := dskv.NewPRConext(dskv.GetMaxBackoff)
		//delete index data
		if err = p.deleteIndexes(context, t, oldIndexKeys); err != nil {
			return
		}

		//insert index data
		if err = p.insertIndexes(context, t, newIndexKvPairs); err != nil {
			return
		}

	}

	// TODO: pool
	sreq := &kvrpcpb.UpdateRequest{
		Key:          key,
		Scope:        scope,
		Fields:       exprs,
		WhereFilters: pbMatches,
		Limit:        pbLimit,
	}
	return p.updateRemote(t, sreq)
}

func getSelectFieldsForIndex(t *Table, exprs []*kvrpcpb.Field) ([]*kvrpcpb.SelectField, map[string]*kvrpcpb.Field) {
	var (
		indexColMap  = make(map[string]*kvrpcpb.Field, 0)
		selectFields []*kvrpcpb.SelectField
	)
	for _, expr := range exprs {
		if expr.Column.Index {
			indexColMap[expr.Column.Name] = expr
		}
	}
	if len(indexColMap) == 0 {
		return nil, nil
	}
	for _, pkColName := range t.PKS() {
		pkCol := t.FindColumn(pkColName)
		selectFields = append(selectFields, &kvrpcpb.SelectField{
			Typ:    kvrpcpb.SelectField_Column,
			Column: pkCol,
		})
	}
	for _, expr := range indexColMap {
		selectFields = append(selectFields, &kvrpcpb.SelectField{
			Typ:    kvrpcpb.SelectField_Column,
			Column: expr.Column,
		})
	}
	return selectFields, indexColMap
}

func (p *Proxy) updateRemote(t *Table, req *kvrpcpb.UpdateRequest) (affected uint64, err error) {
	context := dskv.NewPRConext(dskv.GetMaxBackoff)
	var errForRetry error
	for metricLoop := 0; ; metricLoop++ {
		if errForRetry != nil {
			errForRetry = context.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("[update]%s execute timeout", context)
				return
			}
		}
		if metricLoop > 0 {
			log.Info("%s, retry update table:%s, loop: %v", context, t.GetName(), metricLoop)
		} else {
			log.Debug("%s, update table:%s, ", context, t.GetName())
		}
		if len(req.Key) != 0 {
			// 单key更新
			affected, err = p.singleUpdateRemote(context, t, req, req.Key)
		} else {
			// 普通的范围更新
			affected, err = p.rangeUpdateRemote(context, t, req)
		}

		if err != nil && err == dskv.ErrRouteChange {
			log.Warn("[update]%s route change ,retry table:%s", context, t.GetName())
			errForRetry = err
			continue
		}
		break
	}
	log.Debug("[update]%s execute finish", context)
	return
}

func (p *Proxy) singleUpdateRemote(context *dskv.ReqContext, t *Table, req *kvrpcpb.UpdateRequest, key []byte) (affected uint64, err error) {
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)

	var resp *kvrpcpb.UpdateResponse
	resp, err = proxy.Update(context, req, key)
	if err != nil {
		return
	}

	if resp.GetCode() == 0 {
		affected = resp.GetAffectedKeys()
	} else {
		err = CodeToErr(int(resp.GetCode()))
	}
	return
}

func (p *Proxy) rangeUpdateRemote(context *dskv.ReqContext, t *Table, req *kvrpcpb.UpdateRequest) (affected uint64, err error) {
	var key, start, end []byte
	var route *dskv.KeyLocation
	var all, rawCount uint64
	scope := req.Scope
	limit := req.Limit
	var subLimit *kvrpcpb.Limit

	start = scope.Start
	end = scope.Limit
	var rangeCount int
	for {
		if key == nil {
			key = start
		} else if route != nil {
			key = route.EndKey
			// check key in range
			if bytes.Compare(key, start) < 0 || bytes.Compare(key, end) >= 0 {
				// 遍历完成，直接退出循环
				break
			}
		}
		if limit != nil {
			if limit.Count > all {
				rawCount = limit.Count - all
			} else {
				break
			}
			subLimit = &kvrpcpb.Limit{Count: rawCount}
			log.Debug("limit %v", subLimit)
		}
		req := &kvrpcpb.UpdateRequest{
			Scope:        scope,
			Fields:       req.Fields,
			WhereFilters: req.WhereFilters,
			Limit:        subLimit,
		}

		var affectedTp uint64
		affectedTp, err = p.singleUpdateRemote(context, t, req, key)
		if err != nil {
			return
		}

		rangeCount++
		affected += affectedTp
	}

	if rangeCount >= 3 {
		log.Warn("[update]request to too much ranges(%d): req: %v", rangeCount, req)
	}

	return
}
