package server

import (
	"fmt"
	"bytes"

	"util"
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
	var (
		key       []byte
		scope     *kvrpcpb.Scope
		pbMatches []*kvrpcpb.Match
		pbLimit   *kvrpcpb.Limit
	)
	pbMatches, err = makePBMatches(t, matches)
	if err != nil {
		log.Error("[update]covert filter failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
		return
	}
	pbLimit, err = makePBLimit(p, limit)
	if err != nil {
		log.Error("[update]covert limit failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
		return
	}

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
	selectFields, idxNewValExprMap := getSelectFieldsFromExprs(t, exprs)
	indexCount := len(idxNewValExprMap)
	if indexCount > 0 {
		log.Debug("[update]start to retrieve index data, indexCount: %v", indexCount)
		selectColMap := make(map[string]int, len(selectFields))
		for i, filed := range selectFields {
			selectColMap[filed.Column.Name] = i
		}

		//retrieve: pk value and index field old value
		//index1(old),...,indexn(old),pk1,...,pkn, correspond to selectFields
		var (
			pksAndOldIdxData [][]*Row
			oldIndexKeys     [][]byte
			newIndexKvPairs  []*kvrpcpb.KeyValue
		)
		sreq := &kvrpcpb.SelectRequest{
			Key:          key,
			Scope:        scope,
			FieldList:    selectFields,
			WhereFilters: pbMatches,
			Limit:        pbLimit,
		}
		pksAndOldIdxData, err = p.selectRemote(t, sreq)
		if err != nil {
			log.Error("[update]selectRemoteForIndex error: %v", err)
			return
		}
		for _, partRData := range pksAndOldIdxData {
			for _, rData := range partRData {
				fmt.Println(fmt.Sprintf("[update]select index row data: %v", rData))
				for i := 0; i < indexCount; i++ {
					var (
						oldKey           []byte
						idxOldValue      []byte
						newKey, newValue []byte
					)
					col := selectFields[i].Column
					idxOldValue, err = formatValue(rData.fields[i].value)
					if err != nil {
						log.Error("[update]field %v value %v change to byte array err:%v", rData.fields[i].col, rData.fields[i].value, err)
						return
					}
					oldKey, err = encodeUniqueIndexKey(t, col, idxOldValue)
					if err != nil {
						log.Error("[update]encode unique index old key for table[%v:%v] err: %v", t.GetDbId(), t.GetId(), err)
						return
					}
					expr := idxNewValExprMap[col.Name]
					//if expr.FieldType !=  kvrpcpb.FieldType_Assign {
					//	todo expression update of index field
					//  idxNewValue :=  expr.Value
					//}
					newKey, err = encodeUniqueIndexKey(t, col, expr.Value)
					if err != nil {
						log.Error("[update]encode unique index new key for table[%v:%v] err: %v", t.GetDbId(), t.GetId(), err)
						return
					}
					if !col.Unique {
						// 编码主键
						for j := range t.PKS() {
							col := selectFields[indexCount+j].Column
							var pkValue []byte
							pkValue, err = formatValue(rData.fields[indexCount+j].value)
							if err != nil {
								log.Error("[update]field %v value %v change to byte array err: %v", col.Name, rData.fields[indexCount+j].value, err)
								return
							}
							oldKey, err = util.EncodePrimaryKey(oldKey, col, pkValue)
							if err != nil {
								log.Error("[update]encode Primary key for table[%v:%v] err: %v", t.GetDbId(), t.GetId(), err)
								return
							}
							newKey, err = util.EncodePrimaryKey(newKey, col, pkValue)
							if err != nil {
								log.Error("[update]encode Primary key for table[%v:%v] err: %v", t.GetDbId(), t.GetId(), err)
								return
							}

						}
					} else {
						for j := range t.PKS() {
							col := selectFields[indexCount+j].Column
							var pkValue []byte
							pkValue, err = formatValue(rData.fields[indexCount+j].value)
							if err != nil {
								log.Error("[update]pk field %v value %v change to byte array err: %v", col.Name, rData.fields[indexCount+j].value, err)
								return
							}
							newValue, err = util.EncodePrimaryKey(newValue, col, pkValue)
							if err != nil {
								log.Error("[update]encode Primary key for table[%v:%v] err: %v", t.GetDbId(), t.GetId(), err)
								return
							}
						}
					}
					fmt.Println(fmt.Sprintf("[update]assemble old index key: %v, new index Key: %v, new index value: %v", oldKey, newKey, newValue))
					oldIndexKeys = append(oldIndexKeys, oldKey)
					newIndexKvPairs = append(newIndexKvPairs, &kvrpcpb.KeyValue{
						Key:   newKey,
						Value: newValue,
					})
				}
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

func getSelectFieldsFromExprs(t *Table, exprs []*kvrpcpb.Field) ([]*kvrpcpb.SelectField, map[string]*kvrpcpb.Field) {
	var (
		idxNewValExprMap = make(map[string]*kvrpcpb.Field, 0)
		selectFields       []*kvrpcpb.SelectField
	)
	for _, expr := range exprs {
		if expr.Column.Index {
			selectFields = append(selectFields, &kvrpcpb.SelectField{
				Typ:    kvrpcpb.SelectField_Column,
				Column: expr.Column,
			})
			idxNewValExprMap[expr.Column.Name] = expr
		}
	}
	if len(selectFields) > 0 {
		for _, pkColName := range t.PKS() {
			pkCol := t.FindColumn(pkColName)
			selectFields = append(selectFields, &kvrpcpb.SelectField{
				Typ:    kvrpcpb.SelectField_Column,
				Column: pkCol,
			})
		}
	}
	return selectFields, idxNewValExprMap
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
