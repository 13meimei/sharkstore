package server

import (
	"fmt"

	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"
	"util/hack"
	"util/log"
)

// 把来自多个dataserver的多个行转换成最终结果
func buildSelectResult(stmt *sqlparser.Select, rowss [][]*Row, columns []string) (*mysql.Result, error) {
	// 没有记录
	if len(rowss) <= 0 {
		return &mysql.Result{
			Status:    0,
			Resultset: newEmptyResultSet(columns),
		}, nil
	}

	// 把每个dataserver返回的结果转换成mysql.Result
	rs := make([]*mysql.Result, 0)
	for _, rows := range rowss {
		if len(rows) == 0 {
			log.Warn("empty rows")
			continue
		}
		values := make([][]interface{}, len(rows))
		for i, row := range rows {
			values[i] = make([]interface{}, len(row.fields))
			for j, f := range row.fields {
				values[i][j] = f.value
			}
		}
		r, err := buildResultset(nil, columns, values)
		if err != nil {
			log.Error("build result set failed(%v), columns: %v, values: %v", err, columns, values)
			return nil, err
		}
		result := &mysql.Result{
			Status:       0,
			AffectedRows: uint64(len(rows)),
			Resultset:    r,
		}
		rs = append(rs, result)
	}

	// 合并来自多个dataserver的mysql.Result
	return mergeSelectResult(rs, stmt, columns)
}

// make: Empty Set
func newEmptyResultSet(names []string) *mysql.Resultset {
	r := new(mysql.Resultset)
	r.Fields = make([]*mysql.Field, 0, len(names))
	for _, n := range names {
		r.Fields = append(r.Fields, &mysql.Field{Name: hack.Slice(n)})
	}
	r.Values = make([][]interface{}, 0)
	r.RowDatas = make([]mysql.RowData, 0)
	return r
}

// 把一个dataserver的结果转换为ResultSet
func buildResultset(fields []*mysql.Field, names []string, values [][]interface{}) (*mysql.Resultset, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("empty row value when build select resultset")
	}

	r := new(mysql.Resultset)

	// 设置列信息 r.Fields、 r.FieldNames
	r.Fields = make([]*mysql.Field, 0, len(names))
	r.FieldNames = make(map[string]int, len(names))
	if len(fields) != 0 { // 直接传了schema里的列信息
		if len(fields) != len(names) { //  传的fields和names长度不一致
			return nil, fmt.Errorf("inconsistent length between schema fields and column names")
		}
		r.Fields = append(r.Fields, fields...)
		for i, f := range fields {
			r.FieldNames[string(f.Name)] = i
		}
	} else { // 没传, 根据names和values里(第一行)的列值的类型来构造
		if len(values[0]) != len(names) { // values的列个数跟names长度不一致
			return nil, fmt.Errorf("inconsistent length between field values and column names")
		}
		for i, n := range names {
			field := &mysql.Field{Name: hack.Slice(n)}
			if err := formatField(field, values[0][i]); err != nil {
				return nil, err
			}
			r.Fields = append(r.Fields, field)
			r.FieldNames[n] = i
		}
	}

	var b []byte
	var err error
	// 编码每行的值
	for i, vs := range values {
		if len(vs) != len(r.Fields) {
			return nil, fmt.Errorf("row %d has %d column not equal %d", i, len(vs), len(r.Fields))
		}
		var row []byte
		for _, value := range vs {
			b, err = formatValue(value)
			if err != nil {
				return nil, err
			}
			row = append(row, mysql.PutLengthEncodedString(b)...)
		}
		r.RowDatas = append(r.RowDatas, row)
	}
	//assign the values to the result
	r.Values = values

	return r, nil
}

// 合并来自多个不同dataserver的结果
func mergeSelectResult(rs []*mysql.Result, stmt *sqlparser.Select, columns []string) (*mysql.Result, error) {
	if len(rs) == 0 {
		return nil, fmt.Errorf("invalid mysql select result(empty)")
	}

	// TODO: 聚合函数、group by
	if stmt.GroupBy != nil {
		return nil, fmt.Errorf("group by statement is currently not supported")
	}

	res := new(mysql.Result)
	var err error

	// 合并
	funcExprs := getFuncExprs(stmt)
	if len(funcExprs) == 0 {
		res.Resultset = new(mysql.Resultset)
		res.FieldNames = rs[0].FieldNames
		res.Fields = rs[0].Fields
		for _, r := range rs {
			res.Status |= r.Status
			for _, v := range r.Values {
				res.Values = append(res.Values, v)
			}
			for _, d := range r.RowDatas {
				res.RowDatas = append(res.RowDatas, d)
			}
		}
	} else {
		res.Resultset, err = buildFuncExprResult(stmt, rs, funcExprs, columns)
		if err != nil {
			return nil, err
		}
	}

	// order by
	if err = sortSelectResult(res.Resultset, stmt); err != nil {
		return nil, fmt.Errorf("sort select result failed(%v)", err)
	}

	return res, nil
}

// 排序，处理order by
func sortSelectResult(r *mysql.Resultset, stmt *sqlparser.Select) error {
	if stmt.OrderBy == nil {
		return nil
	}

	sk := make([]mysql.SortKey, len(stmt.OrderBy))

	for i, o := range stmt.OrderBy {
		sk[i].Name = nstring(o.Expr)
		sk[i].Direction = o.Direction
	}

	return r.Sort(sk)
}

func limitSelectResult(r *mysql.Resultset, stmt *sqlparser.Select) error {
	if stmt.Limit == nil {
		return nil
	}

	offset, count, err := parseLimit(stmt.Limit)
	if err != nil {
		return err
	}

	if offset > uint64(len(r.Values)) {
		r.Values = nil
		r.RowDatas = nil
		return nil
	}

	if offset+count > uint64(len(r.Values)) {
		count = uint64(len(r.Values)) - offset
	}

	r.Values = r.Values[offset : offset+count]
	r.RowDatas = r.RowDatas[offset : offset+count]

	return nil
}
