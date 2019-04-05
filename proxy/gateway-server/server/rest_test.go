package server

import (
	"time"
	"fmt"
	"sort"
	"bytes"
	"testing"
	"util"
	"util/log"
	"util/assert"
	"net/http"
	"encoding/json"
	"model/pkg/metapb"
	"model/pkg/txn"
	"proxy/gateway-server/sqlparser"
	"proxy/store/dskv"
)

func TestRestKVHttp(t *testing.T) {
	s, err := mockGwServer()
	if err != nil {
		t.Fatalf("init server failed, err[%v]", err)
	}
	go s.Run()

	//id: auto increment, tinyint
	//name: varchar
	//balance: float
	columnNames := []string{"id", "name", "balance"}

	setQueryRep := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			Values: [][]interface{}{
				[]interface{}{1, "myname1", 0.1},
				[]interface{}{2, "myname2", 0.2},
				[]interface{}{3, "myname3", 0.2},
				[]interface{}{3, "myname3", 0.3},
			},
		},
	}

	dataRep, err := json.Marshal(setQueryRep)
	if err != nil {
		t.Fatal("set query marshal error: ", err)
	}
	//fmt.Println(string(dataRep))
	req, _ := http.NewRequest("POST", "http://127.0.0.1:8080/kvcommand", bytes.NewReader(dataRep))
	req.Header.Set("Content-type", "application/json")

	query, err := httpReadQuery(req)
	if err != nil {
		t.Fatal("http read query error: ", err)
	}
	assert.DeepEqual(t, query.DatabaseName, setQueryRep.DatabaseName)
	assert.DeepEqual(t, query.TableName, setQueryRep.TableName)
	t.Log("query command 1: ", query.Command)
	t.Log("query command 2: ", setQueryRep.Command)
	//w := httptest.NewRecorder()
	//s.handleKVCommand(w, req)
	//t.Logf("%v", w)
	table := s.proxy.router.FindTable(testDBName, testTableName)
	if table == nil {
		t.Fatalf("table %s.%s doesn't exist ", testDBName, testTableName)
	}
	testSetCommand(t, query, table, s.proxy, &Reply{
		Code:         0,
		RowsAffected: 4,
	})

	getQueryRep := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
		},
	}
	expected := [][]interface{}{
		[]interface{}{1, "myname1", 0.1},
		[]interface{}{2, "myname2", 0.2},
		[]interface{}{3, "myname3", 0.3},
	}
	assertGetCommand(t, getQueryRep, s.proxy, expected, table)

	setQueryNoPkRep := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: []string{"name", "balance"},
			Values: [][]interface{}{
				[]interface{}{"myname1", 0.1},
				[]interface{}{"myname2", 0.2},
				[]interface{}{"myname3", 0.3},
				[]interface{}{"myname3", 0.4},
			},
		},
	}

	//自增id无法考虑业务自己赋值的主键
	testSetCommand(t, setQueryNoPkRep, table, s.proxy, &Reply{
		Code:         0,
		RowsAffected: 4,
	})

	//getAggreQueryRep := &Query{
	//	DatabaseName: testDBName,
	//	TableName:    testTableName,
	//	Command: &Command{
	//		Field: []string{},
	//		AggreFunc: []*AggreFunc{
	//			&AggreFunc{Function: "count", Field: "*"},
	//			&AggreFunc{Function: "max", Field: "id"},
	//			&AggreFunc{Function: "min", Field: "id"},
	//			&AggreFunc{Function: "sum", Field: "balance"},
	//		},
	//	},
	//}
	//
	//expected := [][]interface{}{
	//	[]interface{}{4, 4, 1, 1.0},
	//}
	//assertGetCommand(t, getAggreQueryRep, s.proxy, expected, table)

	getQueryRep = &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
		},
	}
	expected = [][]interface{}{
		[]interface{}{1, "myname1", 0.1},
		[]interface{}{2, "myname2", 0.2},
		[]interface{}{3, "myname3", 0.3},
		[]interface{}{4, "myname3", 0.4},
	}
	assertGetCommand(t, getQueryRep, s.proxy, expected, table)

	t.Logf("start to select and filter")
	// test where
	getWhereQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			Filter: &Filter_{
				And: []*And{
					&And{
						Field:  &Field_{Column: "id", Value: uint64(1)},
						Relate: ">",
					},
				},
			},
		},
	}
	expected = [][]interface{}{
		[]interface{}{2, "myname2", 0.2},
		[]interface{}{3, "myname3", 0.3},
		[]interface{}{4, "myname3", 0.4},
	}
	assertGetCommand(t, getWhereQuery, s.proxy, expected, table)

	t.Logf("start to select and pks")
	// test pks
	getPksQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			PKs: [][]*And{
				[]*And{
					&And{
						Field:  &Field_{Column: "id", Value: uint64(2)},
						Relate: ">",
					},
				},
				[]*And{
					&And{
						Field:  &Field_{Column: "name", Value: "myname1"},
						Relate: "=",
					},
				},
			},
		},
	}
	expected = [][]interface{}{
		[]interface{}{3, "myname3", 0.3},
		[]interface{}{4, "myname3", 0.4},
		[]interface{}{1, "myname1", 0.1},
	}
	assertGetCommand(t, getPksQuery, s.proxy, expected, table)

	updQueryRep := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			FieldValue: []*FieldValue{
				{Column: "name", Value: "myname11"},
				{Column: "balance", Value: 0.11},
			},
			Filter: &Filter_{
				And: []*And{
					&And{
						Field:  &Field_{Column: "id", Value: uint64(1)},
						Relate: "=",
					},
				},
			},
		},
	}
	testUpdCommand(t, updQueryRep, table, s.proxy, &Reply{
		Code:         0,
		RowsAffected: 1,
	})

	updQueryRep2 := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			FieldValue: []*FieldValue{
				{Column: "name", Value: "myname112"},
				{Column: "balance", Value: 0.11, Relate: "+"},
			},
			Filter: &Filter_{
				And: []*And{
					&And{
						Field:  &Field_{Column: "id", Value: uint64(1)},
						Relate: "=",
					},
				},
			},
		},
	}
	testUpdCommand(t, updQueryRep2, table, s.proxy, &Reply{
		Code:         0,
		RowsAffected: 1,
	})
}

func assertGetCommand(t *testing.T, query *Query, proxy *Proxy, except [][]interface{}, table *Table) {
	reply, err := query.getCommand(proxy, table)
	if err != nil {
		t.Fatalf("query %v error %v", query, err)
	}
	assert.Equal(t, err, nil, fmt.Sprintf("get command err %v", err))
	assert.Equal(t, reply.Code, 0, fmt.Sprintf("get command code %v", reply.Code))
	assert.Equal(t, len(reply.Values), len(except), fmt.Sprintf("get command value %v, except: %v", reply.Values, except))
	t.Logf("get command value %v, except: %v", reply.Values, except)
}

func mockGwServer() (*Server, error) {
	// load config file
	conf := &Config{
		Log: LogConfig{
			Dir:    logPath,
			Module: "gateway",
			Level:  "debug",
		},
		Cluster: ClusterConfig{
			ID:         1,
			ServerAddr: []string{"192.168.150.122:18887"},
		},
		Performance: PerformConfig{
			GrpcInitWinSize: 1024 * 1024 * 10,
			GrpcPoolSize:    1,
			MaxWorkNum:      10,
		},
		MaxClients: 10000,
		MaxLimit:   DefaultMaxRawCount,
		User:       "test",
		Password:   "123456",
		Charset:    "utf8",
		HttpPort:   8080,
		SqlPort:    3360,
	}
	return NewServer(conf)
}

func TestRestMset(t *testing.T) {
	//log.InitFileLog(logPath, "proxy", "debug")
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true, autoInc: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	tt := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, tt.GetId())
	var pks []*metapb.Column
	for _, col := range tt.Columns {
		if col.Name == "id" {
			pks = append(pks, col)
			break
		}
	}
	r := util.BytesPrefix(start)
	rng := &metapb.Range{
		Id:         1,
		TableId:    1,
		StartKey:   r.Start,
		EndKey:     r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng.PrimaryKeys = pks
	p := newTestProxy(db, tt, rng)
	defer CloseMock(p)
	defer p.Close()

	table := p.router.FindTable(testDBName, testTableName)
	columnNames := []string{"id", "name", "balance"}

	// test set
	setQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			Values: [][]interface{}{
				[]interface{}{3, "myname1", 0.003},
				[]interface{}{2, "myname10", 0.002},
				[]interface{}{1, "myname100", 0.001},
			},
		},
	}

	testSetCommand(t, setQuery, table, p, &Reply{
		Code:         0,
		RowsAffected: 3,
	})

	setNoPkQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: []string{"name", "balance"},
			Values: [][]interface{}{
				[]interface{}{"myname1", 0.003},
				[]interface{}{"myname10", 0.002},
				[]interface{}{"myname100", 0.001},
			},
		},
	}

	testSetCommand(t, setNoPkQuery, table, p, &Reply{
		Code:         0,
		RowsAffected: 3,
	})

	// test getll
	sql := "select id, name, balance from " + testTableName
	sqlstmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := sqlstmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("not select stamentent: %s", sql)
	}

	getQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
		},
	}
	filter_ := testGetFilter(t, getQuery, table, p, stmt)

	expected := [][]interface{}{
		[]interface{}{1, "myname1", 0.003},
		[]interface{}{2, "myname10", 0.002},
		[]interface{}{3, "myname100", 0.001},
	}
	testGetCommand(t, table, p, filter_, stmt, expected, nil, nil, columnNames)
	//testProxySelect(t, p, expected, sql)

	// test where
	sql = "select id, name, balance from " + testTableName + " where id>1"
	sqlstmt, err = sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok = sqlstmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("not select stamentent: %s", sql)
	}

	//order := []*Order{
	//	&Order{
	//		By: "name",
	//	},
	//}
	////var order []*Order
	//limit_ := &Limit_{
	//	Offset:   0,
	//	RowCount: 2,
	//}
	//getQuery = &Query{
	//	DatabaseName: testDBName,
	//	TableName:    testTableName,
	//	Command: &Command{
	//		Field: columnNames,
	//		Filter: &Filter_{
	//			And: []*And{
	//				&And{
	//					Field:  &Field_{Column: "id", Value: uint64(1)},
	//					Relate: ">",
	//				},
	//			},
	//			Order: order,
	//			Limit: limit_,
	//		},
	//	},
	//}
	////data, _ := json.Marshal(getQuery)
	////fmt.Println("~~~~~~~~~~~~~ %v", string(data))
	//filter_ = testGetFilter(t, getQuery, table, p, stmt)
	//expected = [][]interface{}{
	//	//[]string{"1", "myname", "0.001"},
	//	[]interface{}{2, "myname2", 0.002},
	//	[]interface{}{3, "myname3", 0.003},
	//}
	//testGetCommand(t, table, p, filter_, stmt, expected, limit_, order, columnNames)
}

func TestRestKVCommand(t *testing.T) {
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	table := makeTestTable(columns)
	var pks []*metapb.Column
	for _, col := range table.Columns {
		if col.Name == "id" {
			pks = append(pks, col)
			break
		}
	}
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())
	r := util.BytesPrefix(start)
	rng := &metapb.Range{
		Id:         1,
		TableId:    1,
		StartKey:   r.Start,
		EndKey:     r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng.PrimaryKeys = pks
	p := newTestProxy(db, table, rng)
	defer CloseMock(p)
	defer p.Close()

	// this is sql insert
	//testProxyInsert(t, p, 1, "insert into " + testTableName + "(id,name,balance) values(1,'myname',0.0075)")

	table_ := p.router.FindTable(testDBName, testTableName)
	columnNames := []string{"id", "name", "balance"}

	// test set
	setQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			Values: [][]interface{}{
				[]interface{}{1, "myname1", 0.001},
			},
		},
	}
	testSetCommand(t, setQuery, table_, p, &Reply{
		Code:         0,
		RowsAffected: 1,
	})

	setQuery = &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			Values: [][]interface{}{
				[]interface{}{2, "myname2", 0.002},
			},
		},
	}
	testSetCommand(t, setQuery, table_, p, &Reply{
		Code:         0,
		RowsAffected: 1,
	})

	setQuery = &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			Values: [][]interface{}{
				[]interface{}{3, "myname3", 0.003},
			},
		},
	}
	testSetCommand(t, setQuery, table_, p, &Reply{
		Code:         0,
		RowsAffected: 1,
	})

	t.Logf("start to select all")
	// test getll
	sql := "select id, name, balance from " + testTableName
	sqlstmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := sqlstmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("not select stamentent: %s", sql)
	}

	getQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
		},
	}
	filter_ := testGetFilter(t, getQuery, table_, p, stmt)

	expected := [][]interface{}{
		[]interface{}{1, "myname", 0.001},
		[]interface{}{2, "myname2", 0.002},
		[]interface{}{3, "myname3", 0.003},
	}
	testGetCommand(t, table_, p, filter_, stmt, expected, nil, nil, columnNames)

	//todo mock ds for where 、limt and so on
	//t.Logf("start to select and filter")
	//// test where
	//sql = "select id, name, balance from " + testTableName + " where id>1"
	//sqlstmt, err = sqlparser.Parse(sql)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//stmt, ok = sqlstmt.(*sqlparser.Select)
	//if !ok {
	//	t.Fatalf("not select stamentent: %s", sql)
	//}
	//
	//getQuery = &Query{
	//	DatabaseName: testDBName,
	//	TableName:    testTableName,
	//	Command: &Command{
	//		Field: []string{"id", "name", "balance"},
	//		Filter: &Filter_{
	//			And: []*And{
	//				&And{
	//					Field:  &Field_{Column: "id", Value: uint64(1)},
	//					Relate: ">",
	//				},
	//			},
	//		},
	//	},
	//}
	//filter_ = testGetFilter(t, getQuery, table_, p, stmt)
	//
	//expected = [][]interface{}{
	//	//[]string{"1", "myname", "0.001"},
	//	[]interface{}{2, "myname2", 0.002},
	//	[]interface{}{3, "myname3", 0.003},
	//}
	//testGetCommand(t, table_, p, filter_, stmt, expected, nil, nil, nil)
	//

	//// test pks
	//t.Logf("start to select and pks")
	//getQuery = &Query{
	//	DatabaseName: testDBName,
	//	TableName:    testTableName,
	//	Command: &Command{
	//		Field: columnNames,
	//		PKs: [][]*And{
	//			[]*And{
	//				&And{
	//					Field:  &Field_{Column: "id", Value: uint64(2)},
	//					Relate: ">",
	//				},
	//			},
	//			[]*And{
	//				&And{
	//					Field:  &Field_{Column: "name", Value: "myname1"},
	//					Relate: "=",
	//				},
	//			},
	//		},
	//	},
	//}
	//
	//reply, err := getQuery.getCommand(p, table_)
	//if err != nil {
	//	t.Fatalf("error %v", err)
	//}
	//t.Logf("getResult====%v", reply)
	//assert.Equal(t, reply.Code, 0, "code")
	//assert.Equal(t, len(reply.Values), 1, "value")
}

//support
func TestRestInsert(t *testing.T) {
	//log.InitFileLog(logPath, "proxy", "debug")
	columns := []*columnInfo{
		&columnInfo{name: "k", typ: metapb.DataType_Varchar, isPK: true},
		&columnInfo{name: "v", typ: metapb.DataType_Varchar},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	tt := makeTestTable(columns)
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, tt.GetId())
	var pks []*metapb.Column
	for _, col := range tt.Columns {
		if col.Name == "k" {
			pks = append(pks, col)
			break
		}
	}
	r := util.BytesPrefix(start)
	rng := &metapb.Range{
		Id:         1,
		TableId:    1,
		StartKey:   r.Start,
		EndKey:     r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng.PrimaryKeys = pks
	p := newTestProxy(db, tt, rng)
	defer CloseMock(p)
	defer p.Close()

	table := p.router.FindTable(testDBName, testTableName)
	columnNames := []string{"k", "v"}

	content, err := ZipFile("../conf/config.zip")
	if err != nil {
		t.Fatalf("read zip file err %v", err)
	}

	// test set
	setQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
			Values: [][]interface{}{
				[]interface{}{"key1", content},
			},
		},
	}

	testSetCommand(t, setQuery, table, p, &Reply{
		Code:         0,
		RowsAffected: 1,
	})

	// test getll
	sql := "select k,v from " + testTableName
	sqlstmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := sqlstmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("not select stamentent: %s", sql)
	}

	getQuery := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			Field: columnNames,
		},
	}
	filter_ := testGetFilter(t, getQuery, table, p, stmt)

	expected := [][]interface{}{
		[]interface{}{"key1", content},
	}
	testGetCommand(t, table, p, filter_, stmt, expected, nil, nil, columnNames)

}

func TestRestUpdate(t *testing.T) {
	log.InitFileLog(logPath, "proxy", "debug")
	columns := []*columnInfo{
		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
		&columnInfo{name: "balance", typ: metapb.DataType_Double},
	}
	db := &metapb.DataBase{Name: testDBName, Id: 1}
	table := makeTestTable(columns)
	var pks []*metapb.Column
	for _, col := range table.Columns {
		if col.Name == "id" {
			pks = append(pks, col)
			break
		}
	}
	start := util.EncodeStorePrefix(util.Store_Prefix_KV, table.GetId())
	r := util.BytesPrefix(start)
	rng := &metapb.Range{
		Id:         1,
		TableId:    1,
		StartKey:   r.Start,
		EndKey:     r.Limit,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 3, Version: 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 2, NodeId: 1}},
	}
	rng.PrimaryKeys = pks
	p := newTestProxy(db, table, rng)
	defer CloseMock(p)
	defer p.Close()

	// this is sql insert
	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(1,'myname',0.0075)")

	table_ := p.router.FindTable(testDBName, testTableName)

	// test upd
	updQueryRep := &Query{
		DatabaseName: testDBName,
		TableName:    testTableName,
		Command: &Command{
			FieldValue: []*FieldValue{
				{Column: "name", Value: "myname11"},
				{Column: "balance", Value: 0.11},
			},
			Filter: &Filter_{
				And: []*And{
					&And{
						Field:  &Field_{Column: "id", Value: uint64(1)},
						Relate: "=",
					},
				},
			},
		},
	}
	testUpdCommand(t, updQueryRep, table_, p, &Reply{
		Code:         0,
		RowsAffected: 1,
	})

	//// test getll
	//columnNames := []string{"id", "name", "balance"}
	//sql := "select id,name,balance from " + testTableName
	//sqlstmt, err := sqlparser.Parse(sql)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//stmt, ok := sqlstmt.(*sqlparser.Select)
	//if !ok {
	//	t.Fatalf("not select stamentent: %s", sql)
	//}
	//getQuery := &Query{
	//	DatabaseName: testDBName,
	//	TableName:    testTableName,
	//	Command: &Command{
	//		Field: columnNames,
	//	},
	//}
	//filter_ := testGetFilter(t, getQuery, table_, p, stmt)
	//
	//expected := [][]interface{}{
	//	[]interface{}{1,"mmyname11",0.075},
	//}
	//testGetCommand(t, table_, p, filter_, stmt, expected, nil, nil, columnNames)
}

//id: pk, autoincrement
//name:unique-index
//age:non-unique-index
//columnNames := []string{"id", "name", "age", "sex"}
func TestRestKVHttpForSecondIndex(t *testing.T) {
	s, err := mockGwServer()
	if err != nil {
		t.Fatalf("init server failed, err[%v]", err)
	}
	go s.Run()

	secondIndexDb := "secondindex"
	secondIndexTable := "maggineindex"

	//case: should error, test insert duplicate unique index
	//setQueryRep := &Query{
	//	DatabaseName: secondIndexDb,
	//	TableName:    secondIndexTable,
	//	Command: &Command{
	//		Field: []string{"name", "age", "sex"},
	//		Values: [][]interface{}{
	//			[]interface{}{"a", 10, "女"},
	//			[]interface{}{"a", 11, "男"},
	//			[]interface{}{"c", 5, "女"},
	//			[]interface{}{"d", 14, "男"},
	//		},
	//	},
	//}

	setQueryRep := &Query{
		DatabaseName: secondIndexDb,
		TableName:    secondIndexTable,
		Command: &Command{
			Field: []string{"name", "age", "sex"},
			Values: [][]interface{}{
				[]interface{}{"a", 10, "女"},
				[]interface{}{"b", 11, "男"},
				[]interface{}{"c", 5, "女"},
				[]interface{}{"d", 14, "男"},
			},
		},
	}
	dataRep, err := json.Marshal(setQueryRep)
	if err != nil {
		t.Fatal("set query marshal error: ", err)
	}
	//fmt.Println(string(dataRep))
	req, _ := http.NewRequest("POST", "http://127.0.0.1:8080/kvcommand", bytes.NewReader(dataRep))
	req.Header.Set("Content-type", "application/json")

	query, err := httpReadQuery(req)
	if err != nil {
		t.Fatal("http read query error: ", err)
	}
	table := s.proxy.router.FindTable(secondIndexDb, secondIndexTable)
	if table == nil {
		t.Fatalf("table %s.%s doesn't exist ", secondIndexDb, secondIndexTable)
	}
	//自主id无法考虑业务自己赋值的主键
	testSetCommand(t, query, table, s.proxy, &Reply{
		Code:         0,
		RowsAffected: 4,
	})

	//select
	getQueryRep := &Query{
		DatabaseName: secondIndexDb,
		TableName:    secondIndexTable,
		Command: &Command{
			Field: []string{"id", "name", "age", "sex"},
		},
	}
	expected := [][]interface{}{
		[]interface{}{1, "a", 10, "女"},
		[]interface{}{2, "b", 11, "男"},
		[]interface{}{3, "c", 5, "女"},
		[]interface{}{4, "d", 14, "男"},
	}
	assertGetCommand(t, getQueryRep, s.proxy, expected, table)

	// test upd
	updQueryRep := &Query{
		DatabaseName: secondIndexDb,
		TableName:    secondIndexTable,
		Command: &Command{
			Type: "upd",
			FieldValue: []*FieldValue{
				{Column: "name", Value: "e"},
				{Column: "age", Value: 24},
			},
			Filter: &Filter_{
				And: []*And{
					&And{
						Field:  &Field_{Column: "id", Value: uint64(2)},
						Relate: "=",
					},
				},
			},
		},
	}
	testUpdCommand(t, updQueryRep, table, s.proxy, &Reply{
		Code:         0,
		RowsAffected: 1,
	})

	// test del
	delQueryRep := &Query{
		DatabaseName: secondIndexDb,
		TableName:    secondIndexTable,
		Command: &Command{
			Type: "del",
			Filter: &Filter_{
				And: []*And{
					&And{
						Field:  &Field_{Column: "id", Value: uint64(2)},
						Relate: "<=",
					},
				},
			},
		},
	}
	testDelCommand(t, delQueryRep, table, s.proxy, &Reply{
		Code:         0,
		RowsAffected: 2,
	})
}

func TestRestKVHttpForSecondIndex2(t *testing.T) {
	s, err := mockGwServer()
	if err != nil {
		t.Fatalf("init server failed, err[%v]", err)
	}
	go s.Run()

	secondIndexDb := "secondindex"
	secondIndexTable := "maggineindex2"

	//id: pk, autoincrement
	//name:unique-index
	//balance:non-unique-index
	//columnNames := []string{"id", "name", "balance", "data", "createtime"}

	setQueryRep := &Query{
		DatabaseName: secondIndexDb,
		TableName:    secondIndexTable,
		Command: &Command{
			Field: []string{"id", "name", "balance", "data", "createtime"},
			Values: [][]interface{}{
				[]interface{}{1, "a", 0.5, []byte{1}, time.Now()},
				[]interface{}{2, "a", 0.5, []byte{1}, time.Now()},
				//[]interface{}{"b", 10, "男"},
				//[]interface{}{"c", 5, "女"},
				//[]interface{}{"d", 14, "男"},
			},
		},
	}

	dataRep, err := json.Marshal(setQueryRep)
	if err != nil {
		t.Fatal("set query marshal error: ", err)
	}
	fmt.Println(string(dataRep))
	req, _ := http.NewRequest("POST", "http://127.0.0.1:8080/kvcommand", bytes.NewReader(dataRep))
	req.Header.Set("Content-type", "application/json")

	query, err := httpReadQuery(req)
	if err != nil {
		t.Fatal("http read query error: ", err)
	}
	table := s.proxy.router.FindTable(secondIndexDb, secondIndexTable)
	if table == nil {
		t.Fatalf("table %s.%s doesn't exist ", secondIndexDb, secondIndexTable)
	}
	//自主id无法考虑业务自己赋值的主键
	testSetCommand(t, query, table, s.proxy, &Reply{
		Code:         0,
		RowsAffected: 2,
	})
}

//pre split range by id value: 1,2,3,4,5,6,7,8,9 when create table
func TestTxnPrepare(t *testing.T) {
	s, err := mockGwServer()
	if err != nil {
		t.Fatalf("init server failed, err[%v]", err)
	}
	go s.Run()

	var (
		sqls = []string{
			"insert into " + testTableName + "(id,name,balance) values(1, 'a', 0.31),(11, 'aa', 0.87),(2, 'b', 0.14)",
			"insert into " + testTableName + "(id,name,balance) values(2, 'b', 0.03),(3, 'c', 0.7),(1, 'a', 0.5)",
		}
		txArray = make([]*TxObj, 2)
	)

	for i := 0; i < 2; i++ {
		tx := getTx(t, s.proxy, sqls[i], i)
		txArray[i] = tx
		var (
			priIntents      []*txnpb.TxnIntent
			secIntentsGroup [][]*txnpb.TxnIntent
		)
		ctx := dskv.NewPRConext(int(50 * 1000))
		sort.Sort(TxnIntentSlice(tx.getTxIntents()))
		priIntents, secIntentsGroup, err = regroupIntentsByRange(ctx, tx.GetTable(), tx.getTxIntents())
		if err != nil {
			t.Fatalf("regroup intent by range err %v", err)
		}

		err = tx.preparePrimaryIntents(ctx, priIntents, secIntentsGroup)
		if err != nil {
			t.Fatalf("[commit]prepare tx %v primary intents error %v", tx.GetTxId(), err)
		}

		err = tx.prepareSecondaryIntents(ctx, secIntentsGroup)
		if err != nil {
			t.Fatalf("[commit]txn %v prepare secondary intents err %v", tx.GetTxId(), err)
		}
		log.Debug("prepare tx %v secondary intents success", tx.GetTxId())
		log.Debug("tx %v prepare success", tx.GetTxId())
	}

	for i, tx := range txArray {
		ctx := dskv.NewPRConext(int(50 * 1000))
		err = tx.decidePrimaryKey(ctx, txnpb.TxnStatus_COMMITTED)
		if i == 0 {
			if err == nil {
				t.Fatalf("decide tx %v  should conflict", tx.GetTxId())
			} else {
				log.Info("decide tx %v  err: %v", tx.GetTxId(), err)
			}

		} else {
			if err != nil {
				t.Fatalf("decide tx %v  err: %v", tx.GetTxId(), err)
			} else {
				log.Info("decide tx %v  success", tx.GetTxId())
			}
		}
	}
}

func getTx(t *testing.T, proxy *Proxy, sql string, index int) *TxObj {
	table, intents := testProxyInsert(t, proxy, 3, sql)
	intents[0].IsPrimary = true
	tx := &TxObj{
		txId:       fmt.Sprintf("%v", index+1),
		primaryKey: intents[0].GetKey(),
		implicit:   false,
		intents:    intents,
		table:      table,
		proxy:      proxy,
		Timeout:    50,
	}
	return tx
}

