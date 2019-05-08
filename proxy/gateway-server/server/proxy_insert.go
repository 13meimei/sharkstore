package server

import (
	"bytes"
	"fmt"
	"model/pkg/kvrpcpb"
	"model/pkg/txn"
	"pkg-go/ds_client"
	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"
	"proxy/store/dskv"
	"sort"
	"strconv"
	"time"
	"util"
	"util/hack"
	"util/log"
)

/**
return: table, txIntents, mysql.Result(affectedRows, lastInsertId), error
*/
func (p *Proxy) HandleInsert(db string, stmt *sqlparser.Insert, args []interface{}) (
	t *Table, intents []*txnpb.TxnIntent, res *mysql.Result, err error) {

	parser := &StmtParser{}
	// 解析表名
	tableName := parser.parseTable(stmt)
	t = p.router.FindTable(db, tableName)
	if t == nil {
		err = fmt.Errorf("Table '%s.%s' doesn't exist ", db, tableName)
		log.Error("[insert] find table err: %v", err)
		return
	}

	// 解析插入列名
	cols, err := parser.parseInsertCols(stmt)
	if err != nil {
		log.Error("[insert] parse columns error(%v)", err)
		err = fmt.Errorf("handle insert parseColumn err %s", err.Error())
		return
	}
	// 没有指定列名，添加表的所有列
	if len(cols) == 0 {
		columns := t.GetAllColumns()
		if len(columns) == 0 {
			log.Error("[insert] get table(%s.%s) all columns from router failed", db, tableName)
			err = fmt.Errorf("could not get colums info table(%s.%s)", db, tableName)
			return
		}
		for _, c := range columns {
			cols = append(cols, c.Name)
		}
	}

	// 解析插入行值（可能有多行）
	var rows []InsertRowValue
	rows, err = parser.parseInsertValues(stmt)
	if err != nil {
		log.Error("[insert] table %s.%s parse row values error(%v)", db, tableName, err)
		err = fmt.Errorf("handle insert parseRow err %s", err.Error())
		return
	}
	// 检查每行值的个数跟列名个数是否相等
	for i, r := range rows {
		if len(r) != len(cols) {
			log.Error("[insert] table %s.%s Column count doesn't match value count at row %d(%d != %d)", db, tableName, i, len(r), len(cols))
			err = fmt.Errorf("Column count doesn't match value count at row %d", i)
			return
		}
	}

	// 按照表的每个列查找对应列值位置
	colMap, t, err := p.matchInsertValues(t, cols)
	if err != nil {
		log.Error("[insert] table %s.%s match column values error(%v)", db, tableName, err)
		return
	}
	// 检查是否缺少主键列
	pkName, err := p.checkPKMissing(t, colMap)
	if err != nil {
		log.Error("[insert] table %s.%s missing column(%v)", db, tableName, err)
		return
	}
	lasInsertId := uint64(0)
	//填充自增id值
	if len(pkName) > 0 {
		colMap[pkName] = len(colMap)
		var ids []uint64
		ids, err = p.msCli.GetAutoIncId(t.GetDbId(), t.GetId(), uint32(len(rows)))
		if err != nil {
			log.Error("[insert] table %s.%s get auto_increment value err, %v", db, tableName, err)
			return
		}
		if len(ids) != len(rows) {
			log.Error("[insert] table %s.%s get auto_increment value err, %v", db, tableName, err)
			err = fmt.Errorf("get auto increment id size %d not equal insert size %d", len(ids), len(rows))
			return
		}
		for i, row := range rows {
			row = append(row, []byte(fmt.Sprintf("%v", ids[i])))
			rows[i] = row
		}
		//insert multiple rows, returns the value generated for the first inserted row only
		//see http://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id
		lasInsertId = uint64(ids[0])
	}
	affected := uint64(len(rows))
	intents, err = p.insertRows(t, colMap, rows)
	if err != nil {
		log.Error("insert error table[%s:%s], err %s", db, tableName, err.Error())
		return
	}
	res = new(mysql.Result)
	res.AffectedRows = affected
	res.InsertId = lasInsertId
	res.Status = 0
	return
}

// 查找每列对应的列值的偏移，处理自动添加列逻辑
func (p *Proxy) matchInsertValues(t *Table, cols []string) (colMap map[string]int, newTable *Table, err error) {
	newTable = t
	colMap = make(map[string]int)
	unrecognized := make(map[string]int) // 未识别的列，不在原表定义中的列
	for i, c := range cols {
		col := t.FindColumn(c)
		if col != nil { // 存在该列
			if _, ok := colMap[col.Name]; ok { // 重复了
				err = fmt.Errorf("duplicate column(%v) for insert", c)
				return
			}
			colMap[col.Name] = i
		} else { // 表中没有该列
			if _, ok := unrecognized[c]; ok {
				err = fmt.Errorf("duplicate unrecognized column(%v) for insert", c)
				return
			}
			unrecognized[c] = i
		}
	}

	db := t.DbName()
	table := t.Name()

	// 自动添加列
	if len(unrecognized) > 0 {
		addcols := make([]string, 0, len(unrecognized))
		for k := range unrecognized {
			addcols = append(addcols, k)
			log.Info("%s-%s col[%v] is null, prepare add", db, table, k)
		}
		log.Debug("autoAddColumn %v", unrecognized)
		err = p.autoAddColumn(t, addcols)
		if err != nil {
			log.Error("auto add column[%s:%s:%v] failed, err[%v]", db, table, addcols, err)
			return
		}
		newTable = p.router.FindTable(db, table)
		if newTable == nil {
			err = fmt.Errorf("update table %s.%s info failed", db, table)
			return
		}
		for c, index := range unrecognized {
			col := newTable.FindColumn(c)
			if col == nil {
				log.Info("%s-%s col[%s] is null, may be add failure ", db, table, c)
				err = fmt.Errorf("invalid column %s-%s col[%s] ", db, table, c)
				return
			}
			colMap[col.Name] = index
		}
	}
	return
}

// 检查插入时是否少了主键列： 如果缺少的列是自增id，返回要填充的col
func (p *Proxy) checkPKMissing(t *Table, colMap map[string]int) (string, error) {
	var pkName string
	// 是否缺少主键
	for _, pk := range t.PKS() {
		if _, ok := colMap[pk]; !ok {
			if col := t.FindColumn(pk); col != nil && col.AutoIncrement { //表定义时建，限制只能有一个主键列为自增
				pkName = pk
				continue
			}
			return "", fmt.Errorf("pk(%s) is required for insert", pk)
		}
	}
	return pkName, nil
}

// Format of Data Storage Structure:
//  +-----------------------------------------------------+
//  |                  Key                |    Value      |
//  +-----------------------------------------------------+
//  | Store_Prefix_KV + tableId + PKValue | columnValue   |
//  +-----------------------------------------------------+

// EncodeRecordRow: encode business data
func (p *Proxy) encodeRecordRow(t *Table, colMap map[string]int, rowValue InsertRowValue) (*kvrpcpb.KeyValue, error) {
	var (
		key, value []byte
		err        error
	)
	key, err = encodeRecordKey(t, colMap, rowValue)
	if err != nil {
		return nil, err
	}

	// 编码非主键列作为value
	for colName, colIndex := range colMap {
		col := t.FindColumn(colName)
		if col == nil {
			return nil, fmt.Errorf("invalid table(%s) column(%s)", t.GetName(), colName)
		}
		if col.GetPrimaryKey() == 1 {
			continue
		}
		if colIndex >= len(rowValue) {
			return nil, fmt.Errorf("invalid column(%s)", col.Name)
		}
		value, err = util.EncodeColumnValue(value, col, rowValue[colIndex])
		if err != nil {
			return nil, err
		}
	}

	ttl, err := findTTL(colMap, rowValue)
	if err != nil {
		return nil, fmt.Errorf("find row ttl error(%s)", err)
	}

	return &kvrpcpb.KeyValue{
		Key:   key,
		Value: value,
		TTL:   ttl,
	}, nil
}

func (p *Proxy) EncodeRows(t *Table, colMap map[string]int, rows []InsertRowValue) ([]*kvrpcpb.KeyValue, error) {
	var (
		err         error
		rowsKvPairs []*kvrpcpb.KeyValue
	)
	for _, r := range rows {
		var recordKvPair *kvrpcpb.KeyValue
		recordKvPair, err = p.EncodeRow(t, colMap, r)
		if err != nil {
			return nil, err
		}
		rowsKvPairs = append(rowsKvPairs, recordKvPair)

		var tIndexKvPairs []*kvrpcpb.KeyValue
		tIndexKvPairs, err = p.EncodeIndexes(t, colMap, r)
		if err != nil {
			return nil, err
		}
		rowsKvPairs = append(rowsKvPairs, tIndexKvPairs...)
	}
	return rowsKvPairs, nil
}

func verifyUniqueness(rowsKvPairs []*kvrpcpb.KeyValue) bool {
	if len(rowsKvPairs) <= 1 {
		return true
	}
	log.Debug("start to verify uniqueness")
	var checkDupMap = make(map[string]int, 0)
	for _, recordKv := range rowsKvPairs {
		key := string(recordKv.GetKey())
		if _, ok := checkDupMap[key]; !ok {
			checkDupMap[key] = 1
		} else {
			log.Error("verifyUniqueness: key duplicate")
			return false
		}
	}
	return true
}

// EncodeRow: encode business data,
func (p *Proxy) EncodeRow(t *Table, colMap map[string]int, rowValue InsertRowValue) (*kvrpcpb.KeyValue, error) {
	return p.encodeRecordRow(t, colMap, rowValue)
}

func encodeRecordKey(t *Table, colMap map[string]int, rowValue InsertRowValue) ([]byte, error) {
	var (
		err error
		key []byte
	)

	key = util.EncodeStorePrefix(util.Store_Prefix_KV, t.GetId())
	// 编码主键作为key
	key, err = encodePrimaryKeys(t, key, colMap, rowValue)
	if err != nil {
		return nil, err
	}
	return key, nil
}

func encodePrimaryKeys(t *Table, key []byte, colMap map[string]int, rowValue InsertRowValue) ([]byte, error) {
	var value = key
	var err error

	// 编码主键
	for _, pk := range t.PKS() {
		i, ok := colMap[pk]
		if !ok {
			return nil, fmt.Errorf("pk(%s) is missing", pk)
		}
		col := t.FindColumn(pk)
		if col == nil {
			return nil, fmt.Errorf("invalid pk column(%s)", pk)
		}
		if i >= len(rowValue) {
			return nil, fmt.Errorf("invalid pk(%s) value", pk)
		}
		if rowValue[i] == nil {
			return nil, fmt.Errorf("pk(%s) could not be NULL", pk)
		}
		value, err = util.EncodePrimaryKey(value, col, rowValue[i])
		if err != nil {
			log.Error("encode Primary key for table[%v:%v] err: %v", t.GetDbId(), t.GetId(), err)
			return nil, err
		}
	}
	return value, nil
}

func (p *Proxy) batchInsert(context *dskv.ReqContext, t *Table, kvPairs []*kvrpcpb.KeyValue) (affected uint64, duplicateKey []byte, retryKVPairs []*kvrpcpb.KeyValue, err error) {
	// 首先排序,这个很重要
	sort.Sort(KvParisSlice(kvPairs))

	// 按照route的范围划分kv group
	var kvGroup [][]*kvrpcpb.KeyValue
	kvGroup, err = regroupKvPairsByRange(context, t, kvPairs)

	log.Debug("%s, task insert %s group size: %d", context, t.GetName(), len(kvGroup))

	retryKVPairs = make([]*kvrpcpb.KeyValue, 0)

	// 只需要访问一个range
	if len(kvGroup) == 1 {
		affected, duplicateKey, err = p.insert(context, t, kvGroup[0])
		if err != nil && err == dskv.ErrRouteChange {
			retryKVPairs = append(retryKVPairs, kvGroup[0]...)
		}
		return
	}
	startTime := time.Now()
	// for more range batch insert
	var tasks []*InsertTask
	for _, rows := range kvGroup {
		task := GetInsertTask()
		cClone := context.Clone()
		task.init(cClone, p, t, rows)
		err = p.Submit(task)
		if err != nil {
			// release task
			PutInsertTask(task)
			log.Error("submit insert task failed, err[%v]", err)
			return
		}
		tasks = append(tasks, task)
	}
	// 存在部分task不能被回收的问题，但是不会造成内存泄漏
	for _, task := range tasks {
		err_ := task.Wait()
		if err_ != nil {
			err = err_
			if err == dskv.ErrRouteChange {
				retryKVPairs = append(retryKVPairs, task.rows...)
				log.Debug("insert task route change, table:%s, err[%v]", t.GetName(), err)
				PutInsertTask(task)
				continue
			} else {
				log.Error("insert task do failed, table:%s, err[%v]", t.GetName(), err)
				PutInsertTask(task)
				return
			}

		}
		if task.rest.GetDuplicateKey() != nil {
			duplicateKey = task.rest.GetDuplicateKey()
			PutInsertTask(task)
			return
		}
		affected += task.rest.GetAffected()
		PutInsertTask(task)
	}
	context.GetBackOff().CombineTime(int(time.Since(startTime).Nanoseconds() / 1000000))
	log.Debug("%s execute batch task finish", context)
	return
}

// 按照route的范围划分kv group
func regroupKvPairsByRange(context *dskv.ReqContext, t *Table, kvPairs []*kvrpcpb.KeyValue) (kvGroup [][]*kvrpcpb.KeyValue, err error) {
	ggroup := make(map[uint64][]*kvrpcpb.KeyValue)
	for _, kv := range kvPairs {
		log.Debug("task insert add key[%v]", kv.GetKey())
		l, _err := t.ranges.LocateKey(context.GetBackOff(), kv.GetKey())
		if _err != nil {
			err = _err
			log.Warn("locate key failed, err %v", err)
			return
		}
		var (
			group []*kvrpcpb.KeyValue
			ok    bool
		)
		if group, ok = ggroup[l.Region.Id]; !ok {
			group = make([]*kvrpcpb.KeyValue, 0)
			ggroup[l.Region.Id] = group
		}
		group = append(group, kv)

		//map copy value must reset
		ggroup[l.Region.Id] = group
		// 每100个kv切割一下
		if len(group) >= 100 {
			kvGroup = append(kvGroup, group)
			delete(ggroup, l.Region.Id)
		}
	}
	for _, group := range ggroup {
		if len(group) > 0 {
			kvGroup = append(kvGroup, group)
		}
	}
	return
}

func (p *Proxy) insertRows(t *Table, colMap map[string]int, rows []InsertRowValue) (intents []*txnpb.TxnIntent, err error) {
	//encode business record kvPair、unique and non-unique index kvPair
	var (
		kvPairs []*kvrpcpb.KeyValue
	)
	kvPairs, err = p.EncodeRows(t, colMap, rows)
	if err != nil {
		return
	}

	//verify index key uniqueness in a request
	if t.GetPkDupCheck() && !verifyUniqueness(kvPairs) {
		err = fmt.Errorf("table %v: insert duplicate pk or unique index", t.GetName())
		return
	}

	intents = make([]*txnpb.TxnIntent, len(kvPairs))
	for i, kvPair := range kvPairs {
		intent := &txnpb.TxnIntent{
			Typ:         txnpb.OpType_INSERT,
			Key:         kvPair.GetKey(),
			Value:       kvPair.GetValue(),
			CheckUnique: t.GetPkDupCheck(),
			ExpectedVer: 0,
		}
		intents[i] = intent
	}
	//context := dskv.NewPRConext(dskv.InsertMaxBackoff)
	////insert data
	//affected, duplicateKey, err = p.insertRowsWithContext(context, t, rowsKvPairs)
	//if err != nil {
	//	return
	//}
	//log.Debug("[insert]%s execute finish", context)
	return
}

func (p *Proxy) insertRowsWithContext(context *dskv.ReqContext, t *Table, kvPairs []*kvrpcpb.KeyValue) (affected uint64, duplicateKey []byte, err error) {
	var (
		affectedTp     uint64
		duplicateKeyTp []byte
		errTp          error
		errForRetry    error
	)
	for metricLoop := 0; ; metricLoop++ {
		if kvPairs == nil || len(kvPairs) == 0 {
			break
		}
		if errForRetry != nil {
			errForRetry = context.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("[insert]%s execute timeout", context)
				return
			}
		}
		if metricLoop > 0 {
			log.Info("%s, retry insert table:%s, row size:%d, loop: %v", context, t.GetName(), len(kvPairs), metricLoop)
		} else {
			log.Debug("%s, insert table:%s, row size:%d", context, t.GetName(), len(kvPairs))
		}

		if len(kvPairs) > 1 {
			affectedTp, duplicateKeyTp, kvPairs, errTp = p.batchInsert(context, t, kvPairs)
		} else {
			affectedTp, duplicateKeyTp, errTp = p.insert(context, t, kvPairs)
		}

		if errTp != nil && errTp == dskv.ErrRouteChange {
			log.Warn("[insert]%s route change ,retry table:%s, row size:%v", context, t.GetName(), len(kvPairs))
			duplicateKey = duplicateKeyTp
			affected += affectedTp
			errForRetry = errTp
			err = errTp
			continue
		}
		duplicateKey = duplicateKeyTp
		affected += affectedTp
		err = errTp
		break
	}
	return
}

func (p *Proxy) insert(context *dskv.ReqContext, t *Table, rows []*kvrpcpb.KeyValue) (affected uint64, duplicateKey []byte, err error) {
	if len(rows) == 0 {
		err = ErrEmptyRow
		return
	}
	req := &kvrpcpb.InsertRequest{
		Rows:           rows,
		CheckDuplicate: t.PkDupCheck(),
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	//单range写入，包括单行数据，同range多行数据
	var resp *kvrpcpb.InsertResponse
	resp, _, err = proxy.Insert(context, req, rows[0].GetKey())
	if err != nil {
		return
	}
	if resp.GetCode() == 0 {
		affected = resp.GetAffectedKeys()
	} else if resp.GetDuplicateKey() != nil {
		duplicateKey = resp.GetDuplicateKey()
	}
	if resp.GetCode() > 0 {
		err = CodeToErr(int(resp.GetCode()))
		return
	}
	return
}

func (p *Proxy) autoAddColumn(t *Table, cols []string) error {
	if err := p.router.addColumnToRemote(t.GetDbId(), t.GetId(), cols); err != nil {
		return err
	}
	db := p.router.findDBById(t.GetDbId())
	if db != nil {
		_t, err := db.loadTableFromRemote(t.GetName())
		if err != nil {
			return err
		}
		if t != nil {
			table := NewTable(_t, db.cli, 5*time.Minute)
			table.ranges = t.ranges
			db.AddTable(table)
		}
	}
	return nil
}

type KvParisSlice []*kvrpcpb.KeyValue

func (p KvParisSlice) Len() int {
	return len(p)
}

func (p KvParisSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p KvParisSlice) Less(i int, j int) bool {
	return bytes.Compare(p[i].GetKey(), p[j].GetKey()) < 0
}

func findTTL(colMap map[string]int, rowValue InsertRowValue) (uint64, error) {
	idx, ok := colMap[util.TTL_COL_NAME]
	if !ok {
		return 0, nil
	}
	if idx >= len(rowValue) {
		return 0, fmt.Errorf("invalid column(%s) pos", util.TTL_COL_NAME)
	}
	ttl, err := strconv.ParseUint(hack.String(rowValue[idx]), 10, 64)
	if err != nil {
		return 0, err
	}
	return ttl, nil
}
