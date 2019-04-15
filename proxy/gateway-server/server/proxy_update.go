package server

import (
	"fmt"
	"bytes"
	"strconv"
	"util"
	"util/log"
	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"
	"proxy/store/dskv"
	"pkg-go/ds_client"
	"model/pkg/kvrpcpb"
	"model/pkg/txn"
	"model/pkg/metapb"
	"errors"
)

// HandleUpdate handle update
func (p *Proxy) HandleUpdate(db string, stmt *sqlparser.Update, args []interface{}) (
	t *Table, intents []*txnpb.TxnIntent, res *mysql.Result, err error) {

	parser := &StmtParser{}
	// 解析表名
	tableName := parser.parseTable(stmt)
	t = p.router.FindTable(db, tableName)
	if t == nil {
		err = fmt.Errorf("Table '%s.%s' doesn't exist ", db, tableName)
		log.Error("[update] find table err: %v", err)
		return
	}

	var fieldList []*kvrpcpb.Field
	fieldList, err = parser.parseUpdateFields(t, stmt)
	if err != nil {
		log.Error("[update] parse update exprs failed: %v", err)
		err = fmt.Errorf("parse update exprs failed: %v", err)
		return
	}

	// 解析where条件
	var matches []Match
	if stmt.Where != nil {
		// TODO: 支持OR表达式
		matches, err = parser.parseWhere(stmt.Where)
		if err != nil {
			log.Error("[update] parse where error(%v)", err.Error())
			return
		}
	}

	var limit *Limit
	if stmt.Limit != nil {
		var offset, count uint64
		offset, count, err = parseLimit(stmt.Limit)
		if err != nil {
			log.Error("[update] parse limit error[%v]", err)
			return
		}
		if offset != 0 {
			log.Error("[update] unsupported limit offset")
			err = fmt.Errorf("parse update limit failed: unsupported limit offset")
			return
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
		log.Debug("update exprs: %v, matchs: %v, limit: %v", fieldList, matches, limit)
	}
	var affected uint64
	intents, affected, err = p.doUpdate(t, fieldList, matches, limit, nil)
	if err != nil {
		return
	}

	res = new(mysql.Result)
	res.AffectedRows = affected
	return
}

func (p *Proxy) doUpdate(t *Table, exprs []*kvrpcpb.Field, matches []Match, limit *Limit, userScope *Scope) (intents []*txnpb.TxnIntent, affected uint64, err error) {
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

	//delete index data, insert index data and row data
	sreq := &txnpb.SelectRequest{
		Key:          key,
		Scope:        scope,
		WhereFilters: pbMatches,
		Limit:        pbLimit,
	}
	intents, affected, err = p.selectForUpdate(t, sreq, exprs)
	return
}

func (p *Proxy) selectForUpdate(t *Table, sreq *txnpb.SelectRequest, exprs []*kvrpcpb.Field) ([]*txnpb.TxnIntent, uint64, error) {
	var (
		intents      []*txnpb.TxnIntent
		affected     uint64
		err          error
		selectFields []*kvrpcpb.SelectField
		colMap       = make(map[string]int, 0)
		results      [][]*txnpb.Row
	)
	selectFields, colMap, err = getSelectFieldsOfTable(t)
	if err != nil {
		return nil, 0, err
	}
	sreq.FieldList = selectFields
	results, err = p.selectRemoteNoDecode(t, sreq)
	if err != nil {
		log.Error("[update]select row error: %v", err)
		return nil, 0, err
	}
	if len(results) == 0 {
		return nil, 0, nil
	}
	var exprMap = make(map[string]*kvrpcpb.Field, 0)
	for _, expr := range exprs {
		exprMap[expr.Column.Name] = expr
	}
	context := dskv.NewPRConext(dskv.GetMaxBackoff)
	for _, partRData := range results {
		for _, rData := range partRData {
			var oldRowValue, newRowValue InsertRowValue
			oldRowValue, newRowValue, err = convertTxnResult(t, sreq.GetFieldList(), rData, exprMap)
			if err != nil {
				log.Error("[update]convert row err: %v", err)
				return nil, 0, err
			}

			var recordKvPair *kvrpcpb.KeyValue
			recordKvPair, err = p.EncodeRow(t, colMap, newRowValue)
			if err != nil {
				log.Error("[update]encode row kv pair err:%v", err)
				return nil, 0, err
			}
			intents = append(intents, &txnpb.TxnIntent{
				Typ:         txnpb.OpType_INSERT,
				Key:         recordKvPair.GetKey(),
				Value:       recordKvPair.GetValue(),
				CheckUnique: false,
				ExpectedVer: rData.GetValue().GetVersion(),
			})
			log.Debug("[update]assemble new row Key: %v, row value: %v", recordKvPair.GetKey(), recordKvPair.GetValue())
			affected += 1

			var tIndexKvPairsForInsert, tIndexKvPairsForDel []*kvrpcpb.KeyValue
			tIndexKvPairsForInsert, tIndexKvPairsForDel, err = p.EncodeIndexesForUpd(t, colMap, oldRowValue, newRowValue)
			if err != nil {
				log.Error("[update]encode index kv pair err:%v", err)
				return nil, 0, err
			}

			for _, idxKvPair := range tIndexKvPairsForDel {
				oldKey := idxKvPair.GetKey()
				//scan index, todo need ds to batch support
				var rVersion uint64
				rVersion, err = p.scanIndex(context, t, oldKey)
				if err != nil {
					return nil, 0, err
				}
				log.Debug("[update]assemble old index key: %v", oldKey)
				intents = append(intents, &txnpb.TxnIntent{
					Typ:         txnpb.OpType_DELETE,
					Key:         oldKey,
					CheckUnique: false,
					ExpectedVer: rVersion,
				})
			}

			for _, idxKvPair := range tIndexKvPairsForInsert {
				log.Debug("[update]assemble new index Key: %v, index value: %v", idxKvPair.GetKey(), idxKvPair.GetValue())
				intents = append(intents, &txnpb.TxnIntent{
					Typ:         txnpb.OpType_INSERT,
					Key:         idxKvPair.GetKey(),
					Value:       idxKvPair.GetValue(),
					CheckUnique: true,
					ExpectedVer: 0,
				})
			}
		}
	}
	return intents, affected, nil
}

func (p *Proxy) scanIndex(ctx *dskv.ReqContext, t *Table, startKey []byte) (version uint64, err error) {
	var (
		req = &txnpb.ScanRequest{
			StartKey: startKey,
			EndKey:   nextComparableBytes(startKey),
		}
		kv    *txnpb.KeyValue
		value []byte
	)
	kv, err = sendScanIndexReq(p, ctx, t, req)
	if err != nil {
		return
	}
	value, err = handleScanRow(p, ctx, t, req, kv)
	if err != nil || value == nil {
		return
	}
	//decode value
	//todo version
	return
}

func sendScanIndexReq(p *Proxy, ctx *dskv.ReqContext, t *Table, req *txnpb.ScanRequest) (*txnpb.KeyValue, error) {
	var (
		resp *txnpb.ScanResponse
		err  error
	)
	resp, err = p.handleTxnScan(ctx, t, req)
	if err != nil {
		return nil, err
	}
	if resp.GetCode() != 0 {
		err = errors.New(fmt.Sprintf("scan response code is err %v", resp.GetCode()))
		log.Error("%v", err)
		return nil, err
	}
	kvs := resp.GetKvs()
	if len(kvs) != 1 {
		err = fmt.Errorf("scan response kv size is not equal 1, error")
		return nil, err
	}
	return kvs[0], nil
}

func handleScanRow(p *Proxy, ctx *dskv.ReqContext, t *Table, sourceReq *txnpb.ScanRequest, kv *txnpb.KeyValue) (value []byte, err error) {
	var (
		kValue  = kv.GetValue()
		kIntent = kv.GetIntent()
	)
	if kIntent != nil {
		//must be secondary row
		var (
			txId       = kIntent.GetTxnId()
			primaryKey = kIntent.GetPrimaryKey()
			status     txnpb.TxnStatus
			txErr      *txnpb.TxnError
		)
		if kIntent.GetTimeout() {
			//try to aborted, async
			status, err, txErr = p.recoverFromSecondary(ctx, txId, primaryKey, t, false)
		} else {
			//GetLockInfo
			status, err, txErr = p.handleGetLockInfo(ctx, txId, primaryKey, t)
		}
		if err != nil {
			return
		}
		//for TxnError_NOT_FOUND
		if txErr != nil {
			//retry read single key
			var kv *txnpb.KeyValue
			kv, err = sendScanIndexReq(p, ctx, t, sourceReq)
			if err != nil {
				return
			}
			//ignore intent, use row value, consider time order
			return kv.GetValue(), nil
		}
		if status == txnpb.TxnStatus_COMMITTED {
			//use row intent
			switch kIntent.GetOpType() {
			case txnpb.OpType_INSERT:
				return kIntent.GetValue(), nil
			default:
				return nil, nil
			}
		} else {
			//ignore intent, use row value
			return kValue, nil
		}
	}
	return kValue, nil
}

func convertTxnResult(t *Table, selectFields []*kvrpcpb.SelectField, rData *txnpb.Row, exprMap map[string]*kvrpcpb.Field) (InsertRowValue, InsertRowValue, error) {
	var (
		rValue                   *Row
		oldRowValue, newRowValue InsertRowValue
		err                      error
	)
	rValue, err = decodeRow(t, selectFields, rData)
	if err != nil {
		log.Error("[update]decode row err: %v", err)
		return nil, nil, err
	}
	log.Debug("[update]select row data: %v", rValue)

	for i, field := range selectFields {
		var (
			col                = field.GetColumn()
			oldValue, newValue []byte
		)
		//deal business data
		if rValue.fields[i].value != nil {
			oldValue, err = formatValue(rValue.fields[i].value)
			if err != nil {
				log.Error("[update]field %v value %v change to byte array err:%v", rValue.fields[i].col, rValue.fields[i].value, err)
				return nil, nil, err
			}
		}
		oldRowValue = append(oldRowValue, oldValue)
		if expr, ok := exprMap[col.GetName()]; ok {
			if newValue, err = computeUpdExpr(col, expr, oldValue); err != nil {
				log.Error("[update]compute field %v expr value err: %v", col.GetName(), rValue.fields[i].value, err)
				return nil, nil, err
			}
			newRowValue = append(newRowValue, newValue)
		} else {
			newRowValue = append(newRowValue, oldValue)
		}
	}
	return oldRowValue, newRowValue, nil
}

func encodeIndexKey(t *Table, col *metapb.Column, colMap map[string]int, idxValue []byte, row *Row) (key []byte, err error) {
	key, err = encodeUniqueIndexKey(t, col, idxValue)
	if err != nil {
		log.Error("encode unique index key for table[%v:%v] err: %v", t.GetDbName(), t.GetName(), err)
		return
	}
	if !col.Unique {
		// 编码主键
		for _, pkName := range t.PKS() {
			colIndex := colMap[pkName]
			var pkValue []byte
			pkValue, err = formatValue(row.fields[colIndex].value)
			if err != nil {
				log.Error("field %v value %v change to byte array err: %v", col.Name, row.fields[colIndex].value, err)
				return
			}
			key, err = util.EncodePrimaryKey(key, col, pkValue)
			if err != nil {
				log.Error("encode Primary key for table[%v:%v] err: %v", t.GetDbName(), t.GetName(), err)
				return
			}
		}
	}
	return
}

func computeUpdExpr(col *metapb.Column, expr *kvrpcpb.Field, oldValue []byte) (newValue []byte, err error) {
	switch expr.GetFieldType() {
	case kvrpcpb.FieldType_Assign:
		newValue = expr.GetValue()
		return
	case kvrpcpb.FieldType_Plus:
		if oldValue == nil {
			newValue = expr.GetValue()
			return
		}
		if len(expr.GetValue()) == 0 {
			newValue = oldValue
			return
		}
	case kvrpcpb.FieldType_Minus:
		compareVal := bytes.Compare(oldValue, expr.GetValue())
		if compareVal == -1 && col.GetUnsigned() {
			err = fmt.Errorf("Out of range value for column '%v' ", col.GetName())
			return
		}
		if compareVal == 0 {
			return
		}
	case kvrpcpb.FieldType_Mult:
		if oldValue == nil {
			return
		}
	case kvrpcpb.FieldType_Div:
		if len(expr.GetValue()) == 0 || string(expr.GetValue()) == "0" {
			err = fmt.Errorf("Out of range value for column '%v' ", col.GetName())
			return
		}
		log.Info("expr value %v", expr.GetValue())
	}
	newValue, err = operate(col, expr, oldValue)
	return
}

func operate(col *metapb.Column, expr *kvrpcpb.Field, oldValue []byte) (result []byte, err error) {
	var (
		valueStr = string(oldValue)
		exprStr  = string(expr.GetValue())
	)
	switch col.DataType {
	case metapb.DataType_Tinyint, metapb.DataType_Smallint, metapb.DataType_Int, metapb.DataType_BigInt:
		if col.Unsigned {
			var value1, value2 int64
			value1, err = strconv.ParseInt(valueStr, 10, 64)
			if err != nil {
				return
			}
			value2, err = strconv.ParseInt(exprStr, 10, 64)
			if err != nil {
				return
			}
			switch expr.GetFieldType() {
			case kvrpcpb.FieldType_Plus:
				result = []byte(fmt.Sprintf("%v", value1+value2))
			case kvrpcpb.FieldType_Minus:
				result = []byte(fmt.Sprintf("%v", value1-value2))
			case kvrpcpb.FieldType_Mult:
				result = []byte(fmt.Sprintf("%v", value1*value2))
			case kvrpcpb.FieldType_Div:
				result = []byte(fmt.Sprintf("%v", value1/value2))
			}
		} else {
			var value1, value2 uint64
			value1, err = strconv.ParseUint(valueStr, 10, 64)
			if err != nil {
				return
			}
			value2, err = strconv.ParseUint(exprStr, 10, 64)
			if err != nil {
				return
			}
			switch expr.GetFieldType() {
			case kvrpcpb.FieldType_Plus:
				result = []byte(fmt.Sprintf("%v", value1+value2))
			case kvrpcpb.FieldType_Minus:
				result = []byte(fmt.Sprintf("%v", value1-value2))
			case kvrpcpb.FieldType_Mult:
				result = []byte(fmt.Sprintf("%v", value1*value2))
			case kvrpcpb.FieldType_Div:
				result = []byte(fmt.Sprintf("%v", value1/value2))
			}
		}
	case metapb.DataType_Float, metapb.DataType_Double:
		var value1, value2 float64
		value1, err = strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return
		}
		value2, err = strconv.ParseFloat(exprStr, 64)
		if err != nil {
			return
		}
		switch expr.GetFieldType() {
		case kvrpcpb.FieldType_Plus:
			result = []byte(fmt.Sprintf("%v", value1+value2))
		case kvrpcpb.FieldType_Minus:
			result = []byte(fmt.Sprintf("%v", value1-value2))
		case kvrpcpb.FieldType_Mult:
			result = []byte(fmt.Sprintf("%v", value1*value2))
		case kvrpcpb.FieldType_Div:
			result = []byte(fmt.Sprintf("%v", value1/value2))
		}
	}
	return
}

func getSelectFieldsOfTable(t *Table) ([]*kvrpcpb.SelectField, map[string]int, error) {
	var err error
	columns := t.GetAllColumns()
	if len(columns) == 0 {
		err = fmt.Errorf("could not get colums info table(%s.%s)", t.GetDbName(), t.GetName())
		log.Error("[update] get table(%s.%s) all columns from router failed", t.GetDbName(), t.GetName())
		return nil, nil, err
	}
	var (
		selectFields []*kvrpcpb.SelectField
		colMap       = make(map[string]int, 0)
	)
	for i, c := range columns {
		selectFields = append(selectFields, &kvrpcpb.SelectField{
			Typ:    kvrpcpb.SelectField_Column,
			Column: c,
		})
		colMap[c.GetName()] = i
	}
	return selectFields, colMap, nil
}

func getSelectFieldsFromExprs(t *Table, exprs []*kvrpcpb.Field) ([]*kvrpcpb.SelectField, map[string]*kvrpcpb.Field) {
	var (
		idxNewValExprMap = make(map[string]*kvrpcpb.Field, 0)
		selectFields     []*kvrpcpb.SelectField
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
