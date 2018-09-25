package server

import (
	"proxy/gateway-server/sqlparser"
	"reflect"
	"testing"
	"util/assert"
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
	cols, err := stparser.parseSelectCols(selectStmt)
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

func TestParseUpdate(t *testing.T)  {
	sql := "update mytable set name ='name2', pass='2323' where id = 2 limit 1"
	parseUpdate(sql, t)
}

func parseUpdate(sql  string, t *testing.T)  {
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
	fileds, err := stparser.parseUpdateFields(updateStmt)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fileds {
		t.Logf("col: %v, value %v", f.col, string(f.value))
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
		t.Logf("limit rowCount %v",  updateStmt.Limit.Rowcount)
	}
}