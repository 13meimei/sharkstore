package server

import (
	"fmt"
	"util"
	"util/log"
	"pkg-go/ds_client"
	"proxy/store/dskv"
	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"
	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"model/pkg/txn"
)

// HandleDelete handle delete
func (p *Proxy) HandleDelete(db string, stmt *sqlparser.Delete, args []interface{}) (
	t *Table, intents []*txnpb.TxnIntent, res *mysql.Result, err error) {
	//var parseTime time.Time
	//start := time.Now()
	//defer func() {
	//	delay := time.Since(start)
	//	trace := sqlparser.NewTrackedBuffer(nil)
	//	stmt.Format(trace)
	//	//p.sqlStats(trace.String(), time.Since(start), time.Since(parseTime))
	//	//p.metric.AddApiWithDelay("delete", true, delay)
	//	if delay > time.Duration(p.config.InsertSlowLog)*time.Millisecond {
	//		log.Info("[delete slow log] %v %v", delay.String(), trace.String())
	//	}
	//}()

	parser := &StmtParser{}

	// 解析表明
	tableName := parser.parseTable(stmt)
	t = p.router.FindTable(db, tableName)
	if t == nil {
		err = fmt.Errorf("Table '%s.%s' doesn't exist ", db, tableName)
		log.Error("[delete] find table err: %v", err)
		return
	}

	var matches []Match
	if stmt.Where != nil {
		matches, err = parser.parseWhere(stmt.Where)
		if err != nil {
			log.Error("handle delete parse where error(%v)", err)
			return
		}
		log.Debug("matches %v", matches)
	}

	//parseTime = time.Now()
	var affected uint64
	intents, affected, err = p.doDelete(t, matches)
	if err != nil {
		return
	}
	res = new(mysql.Result)
	res.AffectedRows = affected
	res.Status = 0
	return
}

func (p *Proxy) doDelete(t *Table, matches []Match) (intents []*txnpb.TxnIntent, affected uint64, err error) {
	var (
		pbMatches []*kvrpcpb.Match
		key       []byte
		scope     *kvrpcpb.Scope
	)
	pbMatches, err = makePBMatches(t, matches)
	if err != nil {
		log.Error("[delete]covert where matches failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
		return
	}
	key, scope, err = findPKScope(t, pbMatches)
	if err != nil {
		log.Error("[delete]get pk scope failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
		return
	}

	//delete index data and row data
	sreq := &txnpb.SelectRequest{
		Key:          key,
		Scope:        scope,
		WhereFilters: pbMatches,
	}
	intents, affected, err = p.selectForDelete(t, sreq)
	return
}

func (p *Proxy) selectForDelete(t *Table, sreq *txnpb.SelectRequest) ([]*txnpb.TxnIntent, uint64, error) {
	var (
		intents       []*txnpb.TxnIntent
		affected      uint64
		err           error
		pksAndIdxData [][]*txnpb.Row
	)
	indexFields := t.AllIndexes()
	indexCount := len(indexFields)
	log.Debug("[delete]start to select data, indexCount: %v", indexCount)
	selectFields, colMap := getSelectFields(t, indexFields)
	//retrieve: pk value and index field old value
	//pk1,...,pkn,index1(old),...,indexn(old), correspond to selectFields
	sreq.FieldList = selectFields
	pksAndIdxData, err = p.selectRemoteNoDecode(t, sreq)
	if err != nil {
		log.Error("[delete]select row error: %v", err)
		return nil, 0, err
	}
	for _, partRData := range pksAndIdxData {
		for _, rData := range partRData {
			var (
				rValue   *Row
				rowKey   = util.EncodeStorePrefix(util.Store_Prefix_KV, t.GetId())
				rVersion = rData.GetValue().GetVersion()
			)
			rValue, err = decodeRow(t, selectFields, rData)
			if err != nil {
				log.Error("[delete]decode row err: %v", err)
				return nil, 0, err
			}
			log.Debug("[delete]select row data: %v", rValue)
			for i, field := range selectFields {
				var (
					col        = field.GetColumn()
					fieldValue []byte
				)
				if rValue.fields[i].value != nil {
					fieldValue, err = formatValue(rValue.fields[i].value)
					if err != nil {
						log.Error("[delete]field %v value %v change to byte array err:%v", rValue.fields[i].col, rValue.fields[i].value, err)
						return nil, 0, err
					}
				}
				if col.GetPrimaryKey() == 1 {
					rowKey, err = util.EncodePrimaryKey(rowKey, col, fieldValue)
					continue
				}
				if col.GetIndex() {
					var indexKey []byte
					log.Info("[delete]index data: field %v, value %v", col.GetName(), fieldValue)
					indexKey, err = encodeIndexKey(t, col, colMap, fieldValue, rValue)
					if err != nil {
						log.Error("[delete]field %v value %v change to byte array err:%v", rValue.fields[i].col, rValue.fields[i].value, err)
						return nil, 0, err
					}
					log.Info("[delete]assemble old index key: %v, new index Key: %v, new index value: %v", indexKey)
					intents = append(intents, &txnpb.TxnIntent{
						Typ:         txnpb.OpType_DELETE,
						Key:         indexKey,
						CheckUnique: false,
						//todo need ds to support: query index version
						//ExpectedVer: rVersion,
						ExpectedVer: 0,
					})
				}
			}
			intents = append(intents, &txnpb.TxnIntent{
				Typ:         txnpb.OpType_DELETE,
				Key:         rowKey,
				CheckUnique: false,
				ExpectedVer: rVersion,
			})
			affected += 1
		}
	}
	return intents, affected, nil
}

func getSelectFields(t *Table, indexCols []*metapb.Column) ([]*kvrpcpb.SelectField, map[string]int) {
	var (
		selectFields []*kvrpcpb.SelectField
		pkColMap     = make(map[string]int, 0)
	)
	for i, pkColName := range t.PKS() {
		pkCol := t.FindColumn(pkColName)
		selectFields = append(selectFields, &kvrpcpb.SelectField{
			Typ:    kvrpcpb.SelectField_Column,
			Column: pkCol,
		})
		pkColMap[pkColName] = i
	}
	pkCount := len(t.PKS())
	for j, col := range indexCols {
		selectFields = append(selectFields, &kvrpcpb.SelectField{
			Typ:    kvrpcpb.SelectField_Column,
			Column: col,
		})
		pkColMap[col.GetName()] = pkCount + j
	}
	return selectFields, pkColMap
}

func (p *Proxy) deleteRemote(db, table string, req *kvrpcpb.DeleteRequest) (uint64, error) {
	t := p.router.FindTable(db, table)
	if t == nil {
		return 0, ErrNotExistTable
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)

	// single delete
	if len(req.Key) > 0 {
		resp, _, err := proxy.Delete(req, req.Key)
		if err != nil {
			return 0, err
		}
		if resp.GetCode() == 0 {
			return resp.GetAffectedKeys(), nil
		} else {
			return 0, fmt.Errorf("remote server return error. Code: %d", resp.Code)
		}
	}

	// batch delete
	resps, err := proxy.SqlDelete(req, req.Scope)
	if err != nil {
		return 0, err
	}
	var affected uint64
	for _, resp := range resps {
		if resp.GetCode() == 0 {
			affected += resp.AffectedKeys
		} else {
			return 0, fmt.Errorf("remote server return error. Code: %d", resp.Code)
		}
	}
	return affected, nil
}
