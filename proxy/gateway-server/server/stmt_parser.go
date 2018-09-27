package server

import (
	"proxy/gateway-server/sqlparser"
	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"util/hack"
	"util/log"
	"strings"
	"strconv"
	"fmt"
)

type SQLValue []byte

type InsertRowValue []SQLValue

type MatchType int

var (
	Invalid  MatchType = 0
	Equal         MatchType = 1
	NotEqual      MatchType = 2
	Less          MatchType = 3
	LessOrEqual   MatchType = 4
	Larger        MatchType = 5
	LargerOrEqual MatchType = 6
)

type FieldType int

var (
	FieldInvalid FieldType = 0
	Plus         FieldType = 1
	Minus        FieldType = 2
	Mult         FieldType = 3
	Div          FieldType = 4
)

type StmtParser struct {
}

type Match struct {
	column    string
	sqlValue  []byte
	matchType MatchType
}

type Limit struct {
	offset   uint64
	rowCount uint64
}

// SelColumn select fields
type SelColumn struct {
	aggreFunc string // empty if not aggregation function
	col       string // empty if select(*) or count(*)
}

type UpdColumn struct {
	column    string
	value     []byte
	fieldType FieldType
} 

type Field struct {
	col   string
	value interface{}
	// aggreFunc string
	aggreCount int64
}

type Row struct {
	fields []Field
}

func (s *StmtParser) parseTable(stmt sqlparser.Statement) string {
	switch v := stmt.(type) {
	case *sqlparser.Select:
		//parse may be more than one table here
		for _, tableExpr := range v.From {
			switch tableIns := tableExpr.(type) {
			case *sqlparser.AliasedTableExpr:
				switch simpTableIns := tableIns.Expr.(type) {
				case *sqlparser.TableName:
					return string(simpTableIns.Name)
				case *sqlparser.Subquery:
					log.Error("is sub query %v", simpTableIns)
				default:
					log.Error("error table name %v", simpTableIns)
				}
			case *sqlparser.JoinTableExpr:
			case *sqlparser.ParenTableExpr:
			default:
				log.Error("error table type %v", tableIns)
			}
		}
	case *sqlparser.Insert:
		return string(v.Table.Name)
	case *sqlparser.Update:
		return string(v.Table.Name)
	case *sqlparser.Delete:
		return string(v.Table.Name)
	case *sqlparser.Replace:
		return string(v.Table.Name)
	case *sqlparser.Truncate:
		return string(v.Table.Name)
	default:
	}
	return ""
}

func (s *StmtParser) parseSelectCols(stmt *sqlparser.Select) (cols []*SelColumn, err error) {
	for _, sexpr := range stmt.SelectExprs {
		switch colExpr := sexpr.(type) {
		case *sqlparser.NonStarExpr: // 非 *
			switch colIns := colExpr.Expr.(type) {
			case *sqlparser.ColName:
				col := &SelColumn{
					col: hack.String(colIns.Name),
				}
				if col.col == "" {
					return nil, fmt.Errorf("invalid column(empty)")
				}
				cols = append(cols, col)
			case *sqlparser.FuncExpr:
				if aggreFunc, aggreCol, err := parseAggreFunc(colIns); err != nil {
					return nil, err
				} else {
					col := &SelColumn{
						aggreFunc: aggreFunc,
						col:       aggreCol,
					}
					cols = append(cols, col)
				}
			default:
				log.Error("error sqlparser.NonStarExpr type %v", colIns)
				err = fmt.Errorf("error sqlparser.NonStarExpr type %v", colIns)
				return
			}
		case *sqlparser.StarExpr:
			col := &SelColumn{}
			cols = append(cols, col)
		default:
			log.Error("error select type %v", colExpr)
			err = fmt.Errorf("error select type %v", colExpr)
			return
		}
	}
	return cols, nil
}

// 解析insert语句的列名部分
func (s *StmtParser) parseInsertCols(stmt *sqlparser.Insert) ([]string, error) {
	colNames := make([]string, 0, len(stmt.Columns))
	for _, col := range stmt.Columns {
		expr, ok := col.(*sqlparser.NonStarExpr)
		if !ok {
			return nil, fmt.Errorf("unsupported insert column type(%T)", expr)
		}
		colName, ok := expr.Expr.(*sqlparser.ColName)
		if !ok {
			return nil, fmt.Errorf("unsupported insert column expr type(%T)", expr.Expr)
		}
		colNames = append(colNames, string(colName.Name))
	}
	return colNames, nil
}

// 解析insert语句插入的值部分
func (s *StmtParser) parseInsertValues(insert *sqlparser.Insert) ([]InsertRowValue, error) {
	var rowValues []InsertRowValue
	valueTuples, ok := insert.Rows.(sqlparser.Values)
	if !ok {
		return nil, fmt.Errorf("unsupported insert rows type: %T", insert.Rows)
	}
	// 插入多行
	for i, tuple := range valueTuples {
		valtuple, ok := tuple.(sqlparser.ValTuple)
		if !ok {
			return nil, fmt.Errorf("unsupported insert value tuple type(%T) at row %d", tuple, i)
		}
		var rowValue []SQLValue
		for j, valExpr := range valtuple {
			// 一行中的某一列的插入值
			switch val := valExpr.(type) {
			case sqlparser.StrVal:
				rowValue = append(rowValue, SQLValue(val))
			case sqlparser.NumVal:
				rowValue = append(rowValue, SQLValue(val))
			case *sqlparser.NullVal:
				rowValue = append(rowValue, SQLValue(nil))
			default:
				return nil, fmt.Errorf("unsupported insert value type(%T) at row %d filed %d", valExpr, i, j)
			}
		}
		rowValues = append(rowValues, rowValue)
	}
	return rowValues, nil
}

func (s *StmtParser) parseUpdateFields(t *Table, update *sqlparser.Update) ([]*kvrpcpb.Field, error) {
	exprs := update.Exprs
	if len(exprs) == 0 {
		// TODO: see mysql error
		return nil, fmt.Errorf("missing update col and value pairs")
	}
	fields := make([]*kvrpcpb.Field, 0, len(exprs))
	for i, expr := range exprs {
		if expr.Name == nil || len(expr.Name.Name) == 0 {
			return nil, fmt.Errorf("missing update col at index %d", i)
		}
		colName := string(expr.Name.Name)
		col := t.FindColumn(colName)
		if col == nil {
			return nil, fmt.Errorf("Unknown column '%s' in 'field list'", colName)
		}
		//检查是否包含主键值，不允许修改
		if col.GetPrimaryKey() == 1 {
			return nil, fmt.Errorf("pk(%s) not allowed for update", colName)
		}

		field := &kvrpcpb.Field{Column: col}
		switch val := expr.Expr.(type) {
		case sqlparser.StrVal:
			field.Value = []byte(val)
		case sqlparser.NumVal:
			field.Value = []byte(val)
		case *sqlparser.NullVal:
			field.Value = nil
		case *sqlparser.BinaryExpr:
			log.Info("binary expr %s%c%s", val.Left, val.Operator, val.Right)
			switch col.DataType {
			case metapb.DataType_Tinyint, metapb.DataType_Smallint, metapb.DataType_Int, metapb.DataType_BigInt:
				fallthrough
			case metapb.DataType_Float, metapb.DataType_Double:
				value, operateType, err := s.parseBinary(col, val)
				if err != nil {
					return nil, err
				}
				field.Value = value
				field.FieldType = kvrpcpb.FieldType(operateType)
			default:
				return nil, fmt.Errorf("unsupported type(%s) when update field(%s)", col.DataType.String(), col.Name)
			}
		default:
			return nil, fmt.Errorf("unsupported update value type(%T) at index %d", expr.Expr, i)
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func (s *StmtParser) parseCompOperator(operator string) MatchType {
	switch operator {
	case sqlparser.AST_EQ:
		return Equal
	case sqlparser.AST_LT:
		return Less
	case sqlparser.AST_GT:
		return Larger
	case sqlparser.AST_LE:
		return LessOrEqual
	case sqlparser.AST_GE:
		return LargerOrEqual
	case sqlparser.AST_NE:
		return NotEqual
	case sqlparser.AST_NSE:
	case sqlparser.AST_IN:
	case sqlparser.AST_NOT_IN:
	case sqlparser.AST_LIKE:
	case sqlparser.AST_NOT_LIKE:
	}
	return Invalid
}

func (s *StmtParser) parseComparison(expr *sqlparser.ComparisonExpr) (*Match, error) {
	var column string
	if col, ok := expr.Left.(*sqlparser.ColName); ok {
		column = string(col.Name)
	} else {
		log.Error("invalid expr")
		return nil, fmt.Errorf("expr.left transfer type err %v", expr.Left)
	}
	matchType := s.parseCompOperator(expr.Operator)
	if matchType == Invalid {
		return nil, fmt.Errorf("unsupported comparsion operator(%s)", expr.Operator)
	}
	var value []byte
	switch lrVal := expr.Right.(type) {
	case sqlparser.StrVal:
		value = []byte(lrVal)
	case sqlparser.NumVal:
		value = []byte(lrVal)
	default:
		log.Debug("unknown val type")
		return nil, fmt.Errorf("expr type unsupported, unknown val type %v", expr.Right)
	}
	return &Match{column: column, sqlValue: value, matchType: matchType}, nil
}

func (s *StmtParser) parseBinaryOperator(operator byte) FieldType {
	switch operator {
	case sqlparser.AST_PLUS:
		return Plus
	case sqlparser.AST_MINUS:
		return Minus
	case sqlparser.AST_MULT:
		return Mult
	case sqlparser.AST_DIV:
		return Div
	}
	return FieldInvalid
}

func (s *StmtParser) parseBinary(colMeta *metapb.Column, expr *sqlparser.BinaryExpr) ([]byte, FieldType, error) {
	var column string
	if col, ok := expr.Left.(*sqlparser.ColName); ok {
		column = string(col.Name)
		log.Info("left col %v, colName %v", column, colMeta.Name)
		if strings.Compare(colMeta.Name, column) != 0 {
			log.Error("invalid expr, expr.left field should be %v not %v", colMeta.Name, expr.Left)
			return nil, FieldInvalid, fmt.Errorf("expr.left field should be %v not %v", colMeta.Name, column)
		}
	} else {
		log.Error("invalid expr, expr.left transfer type err %v", expr.Left)
		return nil, FieldInvalid, fmt.Errorf("expr.left transfer type err %v", expr.Left)
	}

	fieldType := s.parseBinaryOperator(expr.Operator)
	if fieldType == FieldInvalid {
		return nil, FieldInvalid, fmt.Errorf("unsupported binary operator(%c)", expr.Operator)
	}
	var value []byte
	switch lrVal := expr.Right.(type) {
	case sqlparser.NumVal:
		value = []byte(lrVal)
	default:
		log.Debug("unknown val type")
		return nil, FieldInvalid, fmt.Errorf("expr type unsupported, unknown val type %v", expr.Right)
	}
	return value, fieldType, nil
}

func (s *StmtParser) parseMatch(_expr sqlparser.BoolExpr) (matches []Match, err error) {
	switch expr := _expr.(type) {
	case *sqlparser.AndExpr:
		leftMatches, err := s.parseMatch(expr.Left)
		if err != nil {
			return nil, err
		}
		rigthMatches, err := s.parseMatch(expr.Right)
		if err != nil {
			return nil, err
		}
		matches = append(matches, leftMatches...)
		matches = append(matches, rigthMatches...)
	case *sqlparser.ComparisonExpr:
		match, err := s.parseComparison(expr)
		if err != nil {
			return nil, err
		}
		matches = append(matches, *match)
	default:
		return nil, fmt.Errorf("unsupported expr type: %T", expr)
	}
	return
}

// now only support where exp; where exp1 and exp2
func (s *StmtParser) parseWhere(where *sqlparser.Where) ([]Match, error) {
	// TODO: 支持OR表达式
	return s.parseMatch(where.Expr)
}

func parseAggreFunc(expr *sqlparser.FuncExpr) (aggreFunc, aggreCol string, err error) {
	if len(expr.Exprs) != 1 {
		return "", "", fmt.Errorf("invalid aggregate function(%v) arg size(%d)", hack.String(expr.Name), len(expr.Exprs))
	}

	funcName := string(expr.Name)

	switch argExpr := expr.Exprs[0].(type) {
	case *sqlparser.NonStarExpr:
		if colName, ok := argExpr.Expr.(*sqlparser.ColName); !ok {
			return "", "", fmt.Errorf("invalid aggregate function(%v) arg type(%T)", hack.String(expr.Name), argExpr.Expr)
		} else {
			return funcName, string(colName.Name), nil
		}
	case *sqlparser.StarExpr:
		if funcName != "count" { // only count(*)
			return "", "", fmt.Errorf("invalid aggregate function(%v) arg type(%T)", hack.String(expr.Name), argExpr)
		}
		return funcName, "", nil
	default:
		return "", "", fmt.Errorf("invalid aggregate function(%v) arg type(%T)", hack.String(expr.Name), argExpr)
	}
}

func parseLimit(limit *sqlparser.Limit) (offset, count uint64, err error) {
	// limit offset
	if limit.Offset != nil {
		num, ok := limit.Offset.(sqlparser.NumVal)
		if !ok {
			err = fmt.Errorf("invalid limit offset type(%T): not a number", limit.Offset)
			return
		}
		if offset, err = strconv.ParseUint(hack.String([]byte(num)), 10, 64); err != nil {
			err = fmt.Errorf("invalid limit offset(%s): %v", string(num), err)
			return
		}
	}

	// limit count
	num, ok := limit.Rowcount.(sqlparser.NumVal)
	if !ok {
		err = fmt.Errorf("invalid limit count type(%T): not a number", limit.Rowcount)
		return
	}
	if count, err = strconv.ParseUint(hack.String([]byte(num)), 10, 64); err != nil {
		err = fmt.Errorf("invalid limit count(%s): %v", string(num), err)
		return
	}
	return
}

func parseAdminArgs(admin *sqlparser.Admin) (string, []string) {
	cmd := string(admin.Command)
	ret := make([]string, 0, len(admin.Args))
	for _, barg := range admin.Args {
		ret = append(ret, string(barg))
	}
	return cmd, ret
}
