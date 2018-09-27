package server

import (
	"bytes"
	"fmt"

	"model/pkg/metapb"

	"model/pkg/kvrpcpb"
	"util"
	"util/log"
)

var supportedAggreFuncs = map[string]struct{}{
	"min":   struct{}{},
	"max":   struct{}{},
	"sum":   struct{}{},
	"count": struct{}{},
	// TODO:
	// "avg":   struct{}{},
}

func makeFieldList(t *Table, selCols []*SelColumn) ([]*kvrpcpb.SelectField, error) {
	fieldList := make([]*kvrpcpb.SelectField, 0, len(selCols))
	var hasAggre, hasCol bool
	// field list
	for _, sc := range selCols {
		// aggregate function
		if sc.aggreFunc != "" {
			_, ok := supportedAggreFuncs[sc.aggreFunc]
			if !ok {
				return nil, fmt.Errorf("unsupported aggregate function[%s]", sc.aggreFunc)
			}
			var aggreCol *metapb.Column
			// check aggre column exist
			if sc.col == "" && sc.aggreFunc != "count" { // count(*) is ok
				return nil, fmt.Errorf("aggregate column(%s) is required", sc.col)
			}
			if sc.col != "" {
				aggreCol = t.FindColumn(sc.col)
				if aggreCol == nil {
					return nil, fmt.Errorf("Unknown aggregate column '%s' in 'field list'", sc.col)
				}
			}
			fieldList = append(fieldList, &kvrpcpb.SelectField{
				Typ:       kvrpcpb.SelectField_AggreFunction,
				AggreFunc: sc.aggreFunc,
				Column:    aggreCol,
			})
			hasAggre = true
			continue
		}

		hasCol = true
		// select (*)
		if sc.col == "" {
			for _, mc := range t.GetColumns() {
				fieldList = append(fieldList, &kvrpcpb.SelectField{
					Typ:    kvrpcpb.SelectField_Column,
					Column: mc,
				})
			}
			continue
		}

		col := t.FindColumn(sc.col)
		if col == nil {
			return nil, fmt.Errorf("Unknown column '%s' in 'field list'", sc.col)
		}
		fieldList = append(fieldList, &kvrpcpb.SelectField{
			Typ:    kvrpcpb.SelectField_Column,
			Column: col,
		})
	}
	// 有聚合函数没group by时，普通列和聚合函数只能出现一种
	// TODO: 支持group by
	if hasCol && hasAggre {
		return nil, fmt.Errorf("In aggregated query without GROUP BY SELECT list contains nonaggregated column")
	}
	return fieldList, nil
}

func fieldList2ColNames(fieldList []*kvrpcpb.SelectField) ([]string, error) {
	columns := make([]string, 0, len(fieldList))
	for _, f := range fieldList {
		name, err := makeFieldName(f)
		if err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, nil
}

func makeFieldName(f *kvrpcpb.SelectField) (string, error) {
	switch f.Typ {
	case kvrpcpb.SelectField_Column:
		if f.Column == nil {
			return "", fmt.Errorf("column is missing")
		}
		return f.Column.Name, nil
	case kvrpcpb.SelectField_AggreFunction:
		colName := "*"
		if f.Column != nil {
			colName = f.Column.Name
		}
		return fmt.Sprintf("%s(%s)", f.AggreFunc, colName), nil
	default:
		return "", fmt.Errorf("unknown field list type(%v)", f.Typ)
	}
}

func makePBMatches(t *Table, matches []Match) ([]*kvrpcpb.Match, error) {
	if len(matches) == 0 {
		return nil, nil
	}
	pbMatches := make([]*kvrpcpb.Match, 0, len(matches))
	for _, m := range matches {
		col := t.FindColumn(m.column)
		if col == nil {
			log.Error("invalid column[%s %s %s] in where clause", t.DbName(), t.Name(), m.column)
			return nil, fmt.Errorf("Unknown column '%s' in 'where clause'", m.column)
		}
		pbMatches = append(pbMatches, &kvrpcpb.Match{
			Column:    col,
			Threshold: m.sqlValue,
			MatchType: kvrpcpb.MatchType(m.matchType),
		})
	}
	return pbMatches, nil
}

func makePBLimit(p *Proxy, limit *Limit) (*kvrpcpb.Limit, error) {
	var pbLimit *kvrpcpb.Limit
	if limit != nil {
		var count uint64
		//todo 是否需要加限制
		//if limit.rowCount > p.config.MaxLimit {
		//	count = p.config.MaxLimit
		//	return nil, fmt.Errorf("limit must less than %d", p.config.MaxLimit)
		//} else {
			count = limit.rowCount
		//}
		pbLimit = &kvrpcpb.Limit{Offset: limit.offset, Count: count}
	} else {
		pbLimit = &kvrpcpb.Limit{Offset: 0, Count: p.config.MaxLimit}
	}
	return pbLimit, nil
}

// 查找主键约束的范围
// 返回值： key不为nil表示主键完全指定，操作单行
//			scope不为nil表示操作， 操作某个范围内的多行
func findPKScope(t *Table, matches []*kvrpcpb.Match) (key []byte, scope *kvrpcpb.Scope, err error) {
	// 所有主键列都是相等条件约束
	prefix, count, err := findPKPrefix(t, matches)
	if err != nil {
		return nil, nil, err
	}

	prefix = append(util.EncodeStorePrefix(util.Store_Prefix_KV, t.GetId()), prefix...)

	if count == len(t.PKS()) {
		key = prefix
		return
	}

	start, limit, err := findFirstNePKScope(t.PKS()[count], matches)
	if err != nil {
		return nil, nil, err
	}

	// 跟prefix组合
	return nil, concatPKScore(prefix, start, limit), nil
}

// 按主键顺序依次查找相等约束，确定前缀
func findPKPrefix(t *Table, matches []*kvrpcpb.Match) (prefix []byte, count int, err error) {
	for _, pk := range t.PKS() {
		found := false
		for _, m := range matches {
			if m.Column.Name == pk && m.MatchType == kvrpcpb.MatchType_Equal {
				prefix, err = util.EncodePrimaryKey(prefix, m.Column, m.Threshold)
				if err != nil {
					return
				}
				found = true
				count++
				break // 下一个主键列
			}
		}
		// 如果中间有某个主键没找到，则提前停止
		if !found {
			return
		}
	}
	return
}

// 查找第一个没有==条件约束的主键，确定最后的范围
func findFirstNePKScope(pk string, matches []*kvrpcpb.Match) (start, limit []byte, err error) {
	for _, m := range matches {
		if m.Column.Name != pk {
			continue
		}
		b, err := util.EncodePrimaryKey(nil, m.Column, m.Threshold)
		if err != nil {
			return nil, nil, fmt.Errorf("encode pk faile when find pk(%s)'s scope: %v", pk, err)
		}
		switch m.MatchType {
		case kvrpcpb.MatchType_Less:
			if len(limit) == 0 || bytes.Compare(b, limit) < 0 {
				limit = b
			}
		case kvrpcpb.MatchType_LessOrEqual:
			b := nextComparableBytes(b)
			if len(limit) == 0 || len(b) != 0 && bytes.Compare(b, limit) < 0 {
				limit = b
			}
		case kvrpcpb.MatchType_Larger:
			nextB := nextComparableBytes(b)
			if nextB != nil {
				b = nextB
			}
			if len(b) != 0 && bytes.Compare(b, start) > 0 {
				start = b
			}
		case kvrpcpb.MatchType_LargerOrEqual:
			if len(b) != 0 && bytes.Compare(b, start) > 0 {
				start = b
			}
		case kvrpcpb.MatchType_NotEqual:
		default:
			return nil, nil, fmt.Errorf("unsupported match type(%v) when find pk(%s)'s scope", m.MatchType, pk)
		}
	}
	return
}

func concatPKScore(prefix, start, limit []byte) *kvrpcpb.Scope {
	if len(prefix) == 0 {
		return &kvrpcpb.Scope{
			Start: start,
			Limit: limit,
		}
	}

	scope := &kvrpcpb.Scope{}
	scope.Start = append(scope.Start, prefix...)
	if len(start) > 0 {
		scope.Start = append(scope.Start, start...)
	}
	if len(limit) > 0 {
		scope.Limit = append(scope.Limit, prefix...)
		scope.Limit = append(scope.Limit, limit...)
	} else {
		scope.Limit = nextComparableBytes(prefix)
	}
	return scope
}

func nextComparableBytes(b []byte) []byte {
	var result []byte
	for i := len(b) - 1; i >= 0; i-- {
		c := b[i]
		if c < 0xff {
			result = make([]byte, i+1)
			copy(result, b)
			result[i] = c + 1
			return result
		}
	}
	return nil
}

func decodeRow(t *Table, fieldList []*kvrpcpb.SelectField, pbrow *kvrpcpb.Row) (*Row, error) {
	if len(pbrow.Fields) == 0 {
		return nil, nil
	}

	r := &Row{fields: make([]Field, len(fieldList))}
	var val interface{}
	var err error
	data := pbrow.Fields
	for i, f := range fieldList {
		if f.Typ == kvrpcpb.SelectField_AggreFunction && f.AggreFunc == "count" {
			// count的结果类型跟原列类型不一样
			col := &metapb.Column{DataType: metapb.DataType_BigInt, Unsigned: true}
			data, val, err = util.DecodeColumnValue(data, col)
		} else {
			if f.Column == nil {
				err = fmt.Errorf("invalid column at %d", i)
			} else {
				//fmt.Println("ddddddddddddddddecode: ", data, f.Column)
				data, val, err = util.DecodeColumnValue(data, f.Column)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("Table %s.%s decode column(%s) failed(%v)", t.DbName(), t.Name(), f.Column.Name, err)
		}

		colName, err := makeFieldName(f)
		if err != nil {
			return nil, fmt.Errorf("Table %s.%s decode column(%s) failed(%v)", t.DbName(), t.Name(), f.Column.Name, err)
		}

		r.fields[i].col = colName
		r.fields[i].value = val
		// if f.Typ == kvrpcpb.SelectField_AggreFunction {
		// 	r.fields[i].aggreFunc = f.AggreFunc
		// }
		if i < len(pbrow.AggredCounts) {
			r.fields[i].aggreCount = pbrow.AggredCounts[i]
		}
	}
	return r, nil
}

func decodeRows(t *Table, fieldList []*kvrpcpb.SelectField, pbRows [][]*kvrpcpb.Row) ([][]*Row, error) {
	rowss := make([][]*Row, 0, len(pbRows))
	for _, prs := range pbRows {
		rows := make([]*Row, 0, len(prs))
		for _, pr := range prs {
			r, err := decodeRow(t, fieldList, pr)
			if err != nil {
				return nil, err
			}
			if r != nil {
				rows = append(rows, r)
			}
		}
		rowss = append(rowss, rows)
	}
	return rowss, nil
}


func makeUpdFieldList(t *Table, updCols []*UpdColumn) ([]*kvrpcpb.Field, error) {
	fieldList := make([]*kvrpcpb.Field, 0, len(updCols))
	// field list
	for _, uc := range updCols {
		col := t.FindColumn(uc.column)
		if col == nil {
			return nil, fmt.Errorf("Unknown column '%s' in 'field list'", uc.column)
		}
		//检查是否包含主键值，不允许修改
		if col.GetPrimaryKey() == 1 {
			return nil, fmt.Errorf("pk(%s) not allowed for update", uc.column)
		}

		if uc.fieldType != FieldInvalid {
			switch col.DataType {
			case metapb.DataType_Tinyint, metapb.DataType_Smallint, metapb.DataType_Int, metapb.DataType_BigInt:
			case metapb.DataType_Float, metapb.DataType_Double:
			default:
				return nil, fmt.Errorf("unsupported type(%s) when update field(%s)", col.DataType.String(), col.Name)
			}
		}
		fieldList = append(fieldList, &kvrpcpb.Field{
			Column:    col,
			Value:     uc.value,
			FieldType: kvrpcpb.FieldType(uc.fieldType),
		})
	}
	return fieldList, nil
}
