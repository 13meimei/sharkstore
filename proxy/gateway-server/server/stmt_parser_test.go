package server

import (
	"proxy/gateway-server/sqlparser"
	"model/pkg/metapb"
	"reflect"
	"testing"
	"util/assert"
	"time"
)

func TestParseInsertCols(t *testing.T) {
	// 测试解析insert列
	sql := "insert into mytable (id, name) values (1, 'foo')"
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	insert, ok := stmt.(*sqlparser.Insert)
	if !ok {
		t.Fatalf("not insert statemnet")
	}
	stparser := StmtParser{}
	colNames, err := stparser.parseInsertCols(insert)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(colNames, []string{"id", "name"}) {
		t.Fatalf("parse error: expected: [id, name], actual: %v", colNames)
	}

	// 测试解析insert空列
	sql = "insert into mytable values (1, 'foo')"
	stmt, err = sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	insert, ok = stmt.(*sqlparser.Insert)
	if !ok {
		t.Fatalf("not insert statemnet")
	}
	colNames, err = stparser.parseInsertCols(insert)
	if err != nil {
		t.Fatal(err)
	}
	if len(colNames) != 0 {
		t.Fatalf("expected zero length colnames. actual: %v", len(colNames))
	}
}

func TestParseInsertValues(t *testing.T) {
	// 测试解析insert列
	sql := "insert into mytable (id, name) values (1, 'foo')"
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	insert, ok := stmt.(*sqlparser.Insert)
	if !ok {
		t.Fatalf("not insert statemnet")
	}
	stparser := StmtParser{}
	rowValues, err := stparser.parseInsertValues(insert)
	if err != nil {
		t.Fatal(err)
	}
	expected := []InsertRowValue{[]SQLValue{SQLValue("1"), SQLValue("foo")}}
	if !reflect.DeepEqual(rowValues, expected) {
		t.Fatalf("parse error: expected: %v, actual: %v", expected, rowValues)
	}

	// 测试insert多列
	sql = "insert into mytable (id, name) values (1, 'foo'), (2, 'bar')"
	stmt, err = sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	insert, ok = stmt.(*sqlparser.Insert)
	if !ok {
		t.Fatalf("not insert statemnet")
	}
	rowValues, err = stparser.parseInsertValues(insert)
	if err != nil {
		t.Fatal(err)
	}
	expected = []InsertRowValue{
		[]SQLValue{SQLValue("1"), SQLValue("foo")},
		[]SQLValue{SQLValue("2"), SQLValue("bar")},
	}
	if !reflect.DeepEqual(rowValues, expected) {
		t.Fatalf("parse error: expected: %v, actual: %v", expected, rowValues)
	}
}

func TestParseSelectCols(t *testing.T) {
	sql := "select *, a, b, avg(a), count(*) from mytable"
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("not select statemnet")
	}
	stparser := StmtParser{}
	cols, err := stparser.parseSelectCols(selectStmt, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cols {
		t.Logf("col: %v", *c)
	}
	if len(cols) != 5 {
		t.Fatalf("unexpected result length: %v, expected:%v", len(cols), 3)
	}
	if cols[0].col != "" {
		t.Fatal("expected star col")
	}
	if cols[1].col != "a" {
		t.Fatal("expected column name == \"a\"")
	}
	if cols[2].col != "b" {
		t.Fatal("expected column name == \"b\"")
	}
	if cols[3].aggreFunc != "avg" {
		t.Fatal("expected aggregate func == \"avg\"")
	}
	if cols[3].col != "a" {
		t.Fatal("expected aggregate col == \"a\"")
	}
	if cols[4].aggreFunc != "count" {
		t.Fatal("expected aggregate func == \"count\"")
	}
	if cols[4].col != "" {
		t.Fatal("expected aggregate col == \"\"")
	}

}

func TestParseUpdate(t *testing.T) {
	sql := "update mytable set user_name ='name2', pass_word='2323' where h = 2 limit 1"
	parseUpdate(sql, t)

	sql2 := "update mytable set pass_word = age + 1,  pass_word='2' where id = 1"
	parseUpdate(sql2, t)

	sql3 := "update mytable set pass_word = pass_word + '1',  real_name ='2' where id = 1"
	parseUpdate(sql3, t)

	//sql3 := "update mytable set real_name = '1', pass_word='2' where h = 1"
	//parseUpdate(sql3, t)

	//unsupported
	//sql3 := "update mytable set age = name where id = 1"
	//parseUpdate(sql3, t)

	//sql4 := "update mytable set age = concat('a','b')"
	//parseUpdate(sql4, t)

	//sql5 := "update mytable, mytable2 set mytable.age = mytable2.name"
	//parseUpdate(sql5, t)

	//sql6 := "update mytable set name = 'test' where id in (select id from (select * from mytable order by id asc limit 20, 10) AS tt) "
}

func parseUpdate(sql string, t *testing.T) {
	colInfos := []*metapb.Column{
		{Name: "h", Id: uint64(1), DataType: metapb.DataType_BigInt, PrimaryKey: uint64(1), Index: true},
		{Name: "user_name", Id: uint64(2), DataType: metapb.DataType_Varchar, PrimaryKey: uint64(1), Index: true},
		{Name: "pass_word", Id: uint64(3), DataType: metapb.DataType_Varchar, Index: true},
		{Name: "real_name", Id: uint64(4), DataType: metapb.DataType_Varchar, Index: true},
		{Name: "age", Id: uint64(4), DataType: metapb.DataType_Tinyint, Index: true},
	}
	tableMeta := &metapb.Table{
		Name:    "test",
		DbName:  "test",
		DbId:    2,
		Id:      3,
		Columns: colInfos,
		Epoch:   &metapb.TableEpoch{ConfVer: 1, Version: 1},
	}
	ta := NewTable(tableMeta, nil, 5*time.Minute)

	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	updateStmt, ok := stmt.(*sqlparser.Update)
	if !ok {
		t.Fatalf("not update statemnet")
	}
	stparser := StmtParser{}
	tableName := stparser.parseTable(stmt)
	assert.Equal(t, tableName, "mytable", "tableName")
	fileds, err := stparser.parseUpdateFields(ta, updateStmt)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fileds {
		t.Logf("col: %v, value %v, type: %v", f.Column.Name, string(f.Value), f.FieldType)
	}
	if len(fileds) != 2 {
		t.Fatalf("unexpected result length: %v, expected:%v", len(fileds), 2)
	}

	var matchs []Match
	if updateStmt.Where != nil {
		matchs, err = stparser.parseWhere(updateStmt.Where)
		if err != nil {
			t.Errorf("[update] parse where error(%v)", err.Error())
			return
		}
	}
	t.Logf("match %v", matchs)

	if updateStmt.Limit != nil {
		t.Logf("limit rowCount %v", updateStmt.Limit.Rowcount)
	}
}
