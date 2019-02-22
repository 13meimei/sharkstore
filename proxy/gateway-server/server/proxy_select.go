package server

import (
	"fmt"
	"bytes"
	"errors"
	"util/log"
	"model/pkg/kvrpcpb"
	"model/pkg/txn"
	"pkg-go/ds_client"
	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"
	"proxy/store/dskv"
)

func (p *Proxy) HandleSelect(db string, stmt *sqlparser.Select, args []interface{}) (*mysql.Result, error) {
	//var parseTime time.Time
	//start := time.Now()
	//defer func() {
	//	delay := time.Since(start)
	//	trace := sqlparser.NewTrackedBuffer(nil)
	//	stmt.Format(trace)
	//	//p.sqlStats(trace.String(), time.Since(start), time.Since(parseTime))
	//	//p.metric.AddApiWithDelay("select", true, delay)
	//	if delay > time.Duration(p.config.SelectSlowLog)*time.Millisecond {
	//		log.Info("[select slow log %v %v ", delay.String(), trace.String())
	//	}
	//}()
	parser := &StmtParser{}

	// 解析表名
	tableName := parser.parseTable(stmt)
	t := p.router.FindTable(db, tableName)
	if t == nil {
		log.Error("[select] table %s.%s doesn.t exist", db, tableName)
		return nil, fmt.Errorf("Table '%s.%s' doesn't exist", db, tableName)
	}

	// 解析选择列
	cols, err := parser.parseSelectCols(stmt, p.config.AggrEnable)
	if err != nil {
		log.Error("[select] parse colum error: %v", err)
		return nil, fmt.Errorf("handle select parseColumn err %s", err.Error())
	}
	fieldList, err := makeFieldList(t, cols)
	if err != nil {
		log.Error("[select] find %s.%s field list error(%s), ", t.DbName(), t.Name(), err)
		return nil, err
	}

	// 解析where条件
	var matches []Match
	if stmt.Where != nil {
		// TODO: 支持OR表达式
		matches, err = parser.parseWhere(stmt.Where)
		if err != nil {
			log.Error("handle select parse where error(%v)", err.Error())
			return nil, err
		}
	}

	var limit *Limit
	if stmt.Limit != nil {
		offset, count, err := parseLimit(stmt.Limit)
		if err != nil {
			log.Error("select parse limit error[%v]", err)
			return nil, err
		}
		if count > DefaultMaxRawCount {
			log.Warn("limit count exceeding the maximum limit")
			return nil, ErrExceedMaxLimit
		}
		limit = &Limit{offset: offset, rowCount: count}
	}

	if log.GetFileLogger().IsEnableDebug() {
		log.Debug("where %v", stmt.Where)
		log.Debug("have %v", stmt.Having)
		log.Debug("cols %v", cols)
		log.Debug("matches %v", matches)
	}

	//parseTime = time.Now()
	// 向dataserver查询
	rowss, err := p.doSelect(t, fieldList, matches, limit, nil)
	if err != nil {
		return nil, err
	}

	columns, err := fieldList2ColNames(fieldList)
	if err != nil {
		log.Error("[select] Table %s.%s covert field list to column name failed(%v)", t.DbName(), t.Name(), err)
		return nil, fmt.Errorf("covert field list error(%v)", err)
	}

	// 合并结果
	return buildSelectResult(stmt, rowss, columns)
}

func (p *Proxy) doSelect(t *Table, fieldList []*kvrpcpb.SelectField, matches []Match, limit *Limit, userScope *Scope) ([][]*Row, error) {
	var err error

	pbMatches, err := makePBMatches(t, matches)
	if err != nil {
		log.Error("[select]covert filter failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
		return nil, err
	}

	pbLimit, err := makePBLimit(p, limit)
	if err != nil {
		log.Error("[select]covert limit failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
		return nil, err
	}

	var key []byte
	var scope *kvrpcpb.Scope
	if userScope != nil {
		// TODO
		scope = &kvrpcpb.Scope{
			Start: userScope.Start,
			Limit: userScope.End,
		}
	} else {
		key, scope, err = findPKScope(t, pbMatches)
		if err != nil {
			log.Error("[select]get pk scope failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
			return nil, err
		}
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("[select]pk key: [%v], scope: %v", key, scope)
		}
	}
	/** maybe repeat prefix 2017-12-22 dj
	if key != nil {
		key = append(util.EncodeStorePrefix(util.Store_Prefix_KV, t.GetId()), key...)
	}
	*/
	// TODO: pool
	sreq := &txnpb.SelectRequest{
		Key:          key,
		Scope:        scope,
		FieldList:    fieldList,
		WhereFilters: pbMatches,
		Limit:        pbLimit,
	}
	return p.selectRemote(t, sreq)
}

func (p *Proxy) selectRemote(t *Table, req *txnpb.SelectRequest) ([][]*Row, error) {
	var (
		pbRows  [][]*kvrpcpb.Row
		err     error
		context = dskv.NewPRConext(dskv.GetMaxBackoff)
	)
	// single get
	if len(req.Key) != 0 {
		pbRows, err = p.singleSelectRemote(context, t, req, req.GetKey())
	} else {
		// 聚合函数，并行执行, 并且没有limit、offset逻辑
		if len(req.FieldList) > 0 && req.FieldList[0].Typ == kvrpcpb.SelectField_AggreFunction {
			//todo support aggre func
			//pbRows, err = p.selectAggre(t, proxy, req)
			err = errors.New("no support aggre")
		} else { // 普通的范围查询
			pbRows, err = p.rangeSelectRemote(context, t, req)
		}
	}
	if err != nil {
		return nil, err
	}

	return decodeRows(t, req.FieldList, pbRows)
}

func (p *Proxy) singleSelectRemote(ctx *dskv.ReqContext, t *Table, req *txnpb.SelectRequest, key []byte) ([][]*kvrpcpb.Row, error) {
	rows, err := p.selectSingleKey(ctx, t, req, key)
	if err != nil {
		return nil, err
	}
	if log.GetFileLogger().IsEnableDebug() {
		log.Debug("query rows[%v]", rows)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	var resRows []*kvrpcpb.Row
	resRows, err = handleTxRows(p, ctx, t, req, key, rows)
	if err != nil {
		return nil, err
	}
	return [][]*kvrpcpb.Row{resRows}, nil
}

//select single key
func (p *Proxy) selectSingleKey(ctx *dskv.ReqContext, t *Table, req *txnpb.SelectRequest, key []byte) ([]*txnpb.Row, error) {
	var (
		resp *txnpb.SelectResponse
		err  error
	)
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	resp, _, err = proxy.SqlQuery(ctx, req, key)
	if err != nil {
		return nil, err
	}
	if resp.Code > 0 {
		return nil, fmt.Errorf("remote server return error. Code=%d", resp.Code)
	}
	return resp.GetRows(), nil
}

func handleTxRows(p *Proxy, ctx *dskv.ReqContext, t *Table, sourceReq *txnpb.SelectRequest, key []byte, rows []*txnpb.Row) (resRows []*kvrpcpb.Row, err error) {
	resRows = make([]*kvrpcpb.Row, 0)
	for _, row := range rows {
		if row.GetIntent() != nil {
			intentInfo := row.GetIntent()
			//must be secondary row
			if intentInfo.GetTimeout() {
				//try to aborted
				var status txnpb.TxnStatus
				status, err = p.asyncRecoverSecondary(ctx, intentInfo.GetTxnId(), intentInfo.GetPrimaryKey(), t)
				if err != nil {
					return
				}
				if status == txnpb.TxnStatus_COMMITTED {
					//use row intent
					switch intentInfo.GetOpType() {
					case txnpb.OpType_INSERT:
						resRows = append(resRows, &kvrpcpb.Row{Key: row.GetKey(), Fields: intentInfo.GetValue().GetFields()})
					default:
						continue
					}
				} else {
					//use row value
					resRows = append(resRows, &kvrpcpb.Row{Key: row.GetKey(), Fields: row.GetValue().GetFields()})
				}
			} else {
				//GetLockInfo
				var lockResp *txnpb.GetLockInfoResponse
				lockResp, err = p.handleGetLockInfo(ctx, intentInfo.GetTxnId(), intentInfo.GetPrimaryKey(), t)
				if err != nil {
					return
				}
				if lockResp.GetErr() != nil {
					if lockResp.Err.GetErrType() == txnpb.TxnError_NOT_FOUND {
						//retry read single key
						req := &txnpb.SelectRequest{
							Key:       row.GetKey(),
							FieldList: sourceReq.GetFieldList(),
						}
						var tempRows []*txnpb.Row
						tempRows, err = p.selectSingleKey(ctx, t, req, req.GetKey())
						if err != nil {
							return
						}
						//ignore intent, use row value, consider time order
						resRows = append(resRows, &kvrpcpb.Row{Key: row.GetKey(), Fields: tempRows[0].GetValue().GetFields()})
					}
					err = convertTxnErr(lockResp.Err)
					return
				}
				switch lockResp.GetInfo().GetStatus() {
				case txnpb.TxnStatus_COMMITTED:
					//use row intent
					switch row.GetIntent().GetOpType() {
					case txnpb.OpType_INSERT:
						resRows = append(resRows, &kvrpcpb.Row{Key: row.GetKey(), Fields: intentInfo.GetValue().GetFields()})
					default:
						continue
					}
				default:
					//ignore intent, use row value
					resRows = append(resRows, &kvrpcpb.Row{Key: row.GetKey(), Fields: row.GetValue().GetFields()})
				}
			}
		} else {
			resRows = append(resRows, &kvrpcpb.Row{Key: row.GetKey(), Fields: row.GetValue().GetFields()})
		}
	}
	return
}

func (p *Proxy) rangeSelectRemote(context *dskv.ReqContext, t *Table, sreq *txnpb.SelectRequest) ([][]*kvrpcpb.Row, error) {
	var (
		allRows          [][]*kvrpcpb.Row
		err              error
		scope            = sreq.Scope
		limit            = sreq.Limit
		start            = scope.Start
		end              = scope.Limit
		key              []byte
		resp             *txnpb.SelectResponse
		route            *dskv.KeyLocation
		all, count       uint64
		offset, rawCount uint64
		subLimit         *kvrpcpb.Limit
		rangeCount       int
	)
	kvProxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(kvProxy)
	kvProxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
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
			if limit.Offset > all {
				offset = limit.Offset - all
				rawCount = limit.Count
			} else {
				offset = 0
				if limit.Count > (all - limit.Offset) {
					rawCount = limit.Count - (all - limit.Offset)
				} else {
					break
				}
			}
			subLimit = &kvrpcpb.Limit{Offset: offset, Count: rawCount}
			log.Debug("limit %v", subLimit)
		}
		req := &txnpb.SelectRequest{
			Scope:        scope,
			FieldList:    sreq.FieldList,
			WhereFilters: sreq.WhereFilters,
			Limit:        subLimit,
		}
		resp, route, err = kvProxy.SqlQuery(context, req, key)
		if err != nil {
			return nil, err
		}
		if resp.GetCode() != 0 {
			log.Error("remote server return code: %v", resp.GetCode())
			return nil, errors.New(fmt.Sprintf("response code is err %v", resp.GetCode()))
		}
		rangeCount++

		if log.GetFileLogger().IsEnableDebug() {
			if len(resp.GetRows()) > 64 {
				log.Debug("===route %d offset %d rows(%d)", route.Region.Id, resp.GetOffset(), len(resp.GetRows()))
			} else {
				log.Debug("===route %d offset %d rows(%d) %v", route.Region.Id, resp.GetOffset(), len(resp.GetRows()), resp.GetRows())
			}
		}
		var rows []*kvrpcpb.Row
		rows, err = handleTxRows(p, context, t, req, key, resp.GetRows())
		if err != nil {
			return nil, err
		}
		all += resp.GetOffset()

		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("----- offset: %v", resp.GetOffset())
		}
		if limit != nil && (uint64(len(rows))+count >= limit.Count) {
			rows = rows[:limit.Count-count]
			allRows = append(allRows, rows)
			return allRows, nil
		}
		if len(rows) > 0 {
			allRows = append(allRows, rows)
			count += uint64(len(rows))
		}
	}

	if rangeCount >= 3 {
		log.Warn("request to too much ranges(%d): req: %v", rangeCount, sreq)
	}

	return allRows, nil
}
