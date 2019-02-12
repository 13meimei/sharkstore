package server

import (
	"fmt"

	"pkg-go/ds_client"
	"proxy/store/dskv"
	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"
	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"model/pkg/timestamp"
	"util"
	"util/log"
)

// HandleDelete handle delete
func (p *Proxy) HandleDelete(db string, stmt *sqlparser.Delete, args []interface{}) (*mysql.Result, error) {
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
	t := p.router.FindTable(db, tableName)
	if t == nil {
		log.Error("[delete] table %s.%s doesn.t exist", db, tableName)
		return nil, fmt.Errorf("Table '%s.%s' doesn't exist", db, tableName)
	}

	var matchs []Match
	if stmt.Where != nil {
		var err error
		matchs, err = parser.parseWhere(stmt.Where)
		if err != nil {
			log.Error("handle delete parse where error(%v)", err)
			return nil, err
		}
		log.Debug("matchs %v", matchs)
	}

	//parseTime = time.Now()
	affectedRows, err := p.doDelete(t, matchs)
	if err != nil {
		return nil, err
	}
	ret := new(mysql.Result)
	ret.AffectedRows = affectedRows
	ret.Status = 0
	return ret, nil
}

func (p *Proxy) doDelete(t *Table, matches []Match) (affected uint64, err error) {
	pbMatches, err := makePBMatches(t, matches)
	if err != nil {
		log.Error("[delete]covert where matches failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
		return 0, err
	}
	key, scope, err := findPKScope(t, pbMatches)
	if err != nil {
		log.Error("[delete]get pk scope failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
		return 0, err
	}

	//delete index data
	indexFields := t.AllIndexes()
	indexCount := len(indexFields)
	if indexCount > 0 {
		log.Debug("[delete]start to retrieve index data, indexCount: %v", indexCount)
		selectFields := getSelectFields(t, indexFields)
		//retrieve: pk value and index field old value
		//index1(old),...,indexn(old),pk1,...,pkn, correspond to selectFields
		var (
			pksAndOldIdxData [][]*Row
			oldIndexKeys     [][]byte
		)
		sreq := &kvrpcpb.SelectRequest{
			Key:          key,
			Scope:        scope,
			FieldList:    selectFields,
			WhereFilters: pbMatches,
		}
		pksAndOldIdxData, err = p.selectRemote(t, sreq)
		if err != nil {
			log.Error("[delete]selectRemoteForIndex error: %v", err)
			return
		}

		for _, partRData := range pksAndOldIdxData {
			for _, rData := range partRData {
				fmt.Println(fmt.Sprintf("[delete]select index row data: %v", rData))
				for i := 0; i < indexCount; i++ {
					var (
						oldKey      []byte
						idxOldValue []byte
					)
					col := selectFields[i].Column
					fmt.Println(fmt.Sprintf("[delete]index data: field %v, value %v", rData.fields[i].col, rData.fields[i].value))
					idxOldValue, err = formatValue(rData.fields[i].value)
					if err != nil {
						return
					}
					oldKey, err = encodeUniqueIndexKey(t, col, idxOldValue)
					if err != nil {
						return
					}
					if !col.Unique {
						// 编码主键
						for j := range t.PKS() {
							col := selectFields[indexCount+j].Column
							fmt.Println(fmt.Sprintf("[delete]non-unique index data: index %v field %v, value %v", indexCount+j, col.Name, rData.fields[indexCount+j].value))
							var pkValue []byte
							pkValue, err = formatValue(rData.fields[indexCount+j].value)
							oldKey, err = util.EncodePrimaryKey(oldKey, col, pkValue)
							if err != nil {
								log.Error("[delete]encode Primary key for table[%v:%v] err: %v", t.GetDbId(), t.GetId(), err)
								return
							}
						}
					}
					fmt.Println(fmt.Sprintf("[delete]assemble old index key: %v", oldKey))
					oldIndexKeys = append(oldIndexKeys, oldKey)
				}
			}
		}
		context := dskv.NewPRConext(dskv.GetMaxBackoff)
		if err = p.deleteIndexes(context, t, oldIndexKeys); err != nil {
			return
		}
	}

	// TODO: sync pool
	now := p.clock.Now()
	dreq := &kvrpcpb.DeleteRequest{
		Key:          key,
		Scope:        scope,
		WhereFilters: pbMatches,
		Timestamp:    &timestamp.Timestamp{WallTime: now.WallTime, Logical: now.Logical},
	}
	affected, err = p.deleteRemote(t.DbName(), t.Name(), dreq)
	if err != nil {
		log.Error("[delete]delete failed. err: %v, key: %v, scope: %v", err, key, scope)
	} else {
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("[delete]delete success. affected: %v, key: %v, scope: %v", affected, key, scope)
		}
	}
	return
}

func getSelectFields(t *Table, indexCols []*metapb.Column) ([]*kvrpcpb.SelectField) {
	var (
		selectFields []*kvrpcpb.SelectField
	)
	for _, col := range indexCols {
		selectFields = append(selectFields, &kvrpcpb.SelectField{
			Typ:    kvrpcpb.SelectField_Column,
			Column: col,
		})
	}
	for _, pkColName := range t.PKS() {
		pkCol := t.FindColumn(pkColName)
		selectFields = append(selectFields, &kvrpcpb.SelectField{
			Typ:    kvrpcpb.SelectField_Column,
			Column: pkCol,
		})
	}
	return selectFields
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
