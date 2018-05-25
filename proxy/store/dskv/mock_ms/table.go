package mock_ms

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"errors"

	"model/pkg/metapb"
	"util/deepcopy"
	"util/log"
)

var (
	MAX_COLUMN_NAME_LENGTH = 128
)

var PREFIX_AUTO_TRANSFER_TABLE string = fmt.Sprintf("$auto_transfer_table_%d")
var PREFIX_AUTO_FAILOVER_TABLE string = fmt.Sprintf("$auto_failover_table_%d")

type Table struct {
	*metapb.Table
	// 表属性锁
	schemaLock sync.RWMutex
	// 路由锁
	lock     sync.Mutex
	maxColId uint64
	ranges   *RangeCache
}

type rangeToCreate struct {
	startKey []byte
	endKey []byte
}

func NewTable(t *metapb.Table) *Table {
	var maxColId uint64
	for _, col := range t.GetColumns() {
		if col.GetId() > maxColId {
			maxColId = col.GetId()
		}
	}
	table := &Table{
		Table:               t,
		maxColId:            maxColId,
		ranges:              NewRangeCache(),
	}
	return table
}

func (t *Table) GenColId() uint64 {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.maxColId++
	return t.maxColId
}

type TableProperty struct {
	Columns []*metapb.Column `json:"columns"`
	Regxs   []*metapb.Column `json:"regxs"`
}

func (t *Table) Name() string {
	return t.GetName()
}

func (t *Table) GetRangeNumber() int {
	return t.ranges.Size()
}

type ByPrimaryKey []*metapb.Column

func (s ByPrimaryKey) Len() int           { return len(s) }
func (s ByPrimaryKey) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByPrimaryKey) Less(i, j int) bool { return s[i].PrimaryKey < s[j].PrimaryKey }

func (t *Table) GetColumns() []*metapb.Column {
	var columns []*metapb.Column
	_columns := t.Columns
	for _, c := range _columns {
		columns = append(columns, c)
	}
	return columns
}

func (t *Table) GetPkColumns() []*metapb.Column {
	var columns []*metapb.Column
	_columns := t.Columns
	for _, c := range _columns {
		if c.PrimaryKey > 0 {
			columns = append(columns, c)
		}
	}
	return columns
}

func (t *Table) GetColumnByName(name string) (*metapb.Column, bool) {
	_columns := t.Columns
	for _, c := range _columns {
		if c.GetName() == name {
			return c, true
		}
	}
	return nil, false
}

func (t *Table) GetColumnById(id uint64) (*metapb.Column, bool) {
	_columns := t.Columns
	for _, c := range _columns {
		if c.Id == id {
			return c, true
		}
	}
	return nil, false
}

func (t *Table) MergeColumn(source []*metapb.Column, cluster *Cluster) error {
	t.schemaLock.Lock()
	defer t.schemaLock.Unlock()
	table := deepcopy.Iface(t.Table).(*metapb.Table)

	for _, col := range source {
		col.Name = strings.ToLower(col.GetName())

		if len(col.GetName()) > MAX_COLUMN_NAME_LENGTH {
			return ErrColumnNameTooLong
		}

		if col.GetId() == 0 { // add column
			if col.PrimaryKey == 1 {
				log.Warn("pk column is not allow change")
				return ErrInvalidColumn
			}
			if _, find := t.GetColumnByName(col.Name); find {
				log.Warn("column[%s:%s:%s] is already existed", t.GetDbName(), t.GetName(), col.Name)
				return ErrDupColumnName
			}
			col.Id = t.GenColId()
			table.Columns = append(table.Columns, col)
			table.Epoch.ConfVer++
		} else { // column maybe rename
			tt := NewTable(table)
			col_, find := tt.GetColumnById(col.GetId())
			if !find {
				log.Warn("column[%s:%s:%s] is not exist", tt.GetDbName(), tt.GetName(), col.GetId())
				return ErrInvalidColumn
			}
			if col_.Name == col.GetName() {
				continue
			}

			col_.Name = col.GetName()
			table.Epoch.ConfVer++
		}
	}
	t.Table = table
	return nil
}

func (t *Table) AddRange(r *Range) {
	t.ranges.Add(r)
}

func (t *Table) DeleteRange(id uint64) {
	t.ranges.Delete(id)
}

func (t *Table) FindRange(id uint64) (*Range, bool) {
	return t.ranges.FindRangeByID(id)
}

func (t *Table) SearchRange(key []byte) (*Range, bool) {
	return t.ranges.SearchRange(key)
}

func (t *Table) MultipleSearchRanges(key []byte, num int) ([]*Range, bool) {
	return t.ranges.MultipleSearchRanges(key, num)
}

func (t *Table) GetAllRanges() []*Range {
	return t.ranges.GetAllRange()
}

func checkTTLDataType(dataType metapb.DataType) bool {
	return metapb.DataType_BigInt == dataType
}

func (t *Table) UpdateSchema(columns []*metapb.Column) ([]*metapb.Column, error) {
	t.schemaLock.Lock()
	defer t.schemaLock.Unlock()
	table := deepcopy.Iface(t.Table).(*metapb.Table)
	//not concurrent safy,need in syncronized block
	cols := make([]*metapb.Column, 0)
	allCols := make([]*metapb.Column, 0)
	colMap := make(map[string]*metapb.Column)
	for _, tempCol := range table.Columns {
		colMap[tempCol.GetName()] = tempCol
		allCols = append(allCols, tempCol)
	}

	var match bool
	for _, newCol := range columns {
		_, ok := colMap[newCol.GetName()]
		if ok {
			continue
		}
		//scan template column
		for _, tempCol := range table.Regxs {
			isMatch, err := regexp.Match(tempCol.GetName(), []byte(newCol.Name))
			if err == nil && isMatch {
				addCol := deepcopy.Iface(tempCol).(*metapb.Column)
				addCol.Name = newCol.Name
				addCol.Id = t.GenColId()
				cols = append(cols, addCol)
				allCols = append(allCols, addCol)
				match = true
				break
			}
		}
	}
	if match == false {
		return nil, errors.New("none of columns matches")
	}
	props, err := ToTableProperty(allCols)
	if err != nil {
		return nil, err
	}
	table.Columns = allCols
	table.Properties = props
	table.Epoch.ConfVer++
	t.Table = table
	return cols, nil
}

type TableCache struct {
	lock    sync.RWMutex
	tableIs map[uint64]*Table
	tableNs map[string]*Table
}

func NewTableCache() *TableCache {
	return &TableCache{
		tableIs: make(map[uint64]*Table),
		tableNs: make(map[string]*Table)}
}

func (tc *TableCache) Add(t *Table) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.tableIs[t.GetId()] = t
	tc.tableNs[t.GetName()] = t
}

func (tc *TableCache) DeleteById(id uint64) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if t, find := tc.tableIs[id]; find {
		delete(tc.tableIs, t.GetId())
		delete(tc.tableNs, t.GetName())
		return
	}
}

func (tc *TableCache) DeleteByName(name string) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if t, find := tc.tableNs[name]; find {
		delete(tc.tableIs, t.GetId())
		delete(tc.tableNs, t.GetName())
		return
	}
}

func (tc *TableCache) FindTableById(id uint64) (*Table, bool) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	if t, find := tc.tableIs[id]; find {
		return t, true
	}
	return nil, false
}

func (tc *TableCache) FindTableByName(name string) (*Table, bool) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	if t, find := tc.tableNs[name]; find {
		return t, true
	}
	return nil, false
}

func (tc *TableCache) GetAllTable() []*Table {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	var tables []*Table
	for _, t := range tc.tableIs {
		tables = append(tables, t)
	}
	return tables
}

func (tc *TableCache) Size() int {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return len(tc.tableIs)
}

type GlobalTableCache struct {
	lock    sync.RWMutex
	tableIs map[uint64]*Table
}

func NewGlobalTableCache() *GlobalTableCache {
	return &GlobalTableCache{
		tableIs: make(map[uint64]*Table)}
}

func (tc *GlobalTableCache) Add(t *Table) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.tableIs[t.GetId()] = t
}

func (tc *GlobalTableCache) DeleteById(id uint64) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if t, find := tc.tableIs[id]; find {
		delete(tc.tableIs, t.GetId())
		return
	}
}

func (tc *GlobalTableCache) FindTableById(id uint64) (*Table, bool) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	if t, find := tc.tableIs[id]; find {
		return t, true
	}
	return nil, false
}

func (tc *GlobalTableCache) GetAllTable() []*Table {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	var tables []*Table
	for _, t := range tc.tableIs {
		tables = append(tables, t)
	}
	return tables
}

func (tc *GlobalTableCache) Size() int {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return len(tc.tableIs)
}

type CreateTable struct {
	*Table
	rangesToCreateList chan *rangeToCreate
}

func NewCreateTable(t *Table, n uint64) *CreateTable {
	return &CreateTable{
		Table: t,
		rangesToCreateList: make(chan *rangeToCreate, n),
	}
}

type CreateTableCache struct {
	lock    sync.RWMutex
	tableIs map[uint64]*CreateTable
}

func NewCreateTableCache() *CreateTableCache {
	return &CreateTableCache{
		tableIs: make(map[uint64]*CreateTable),
	}
}

func (tc *CreateTableCache) Add(t *CreateTable) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.tableIs[t.GetId()] = t
}

func (tc *CreateTableCache) Delete(id uint64) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if v, find := tc.tableIs[id]; find {
		if v.rangesToCreateList != nil {
			close(v.rangesToCreateList)
		}

		delete(tc.tableIs, id)
		return
	}
}

func (tc *CreateTableCache) FindTable(id uint64) (*CreateTable, bool) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	if t, find := tc.tableIs[id]; find {
		return t, true
	}
	return nil, false
}

func (tc *CreateTableCache) GetAllTable() []*CreateTable {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	var tables []*CreateTable
	for _, t := range tc.tableIs {
		tables = append(tables, t)
	}
	return tables
}

func (tc *CreateTableCache) Size() int {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return len(tc.tableIs)
}

func ToTableProperty(cols []*metapb.Column) (string, error) {
	tp := &TableProperty{
		Columns: make([]*metapb.Column, 0),
	}
	for _, c := range cols {
		tp.Columns = append(tp.Columns, c)
	}
	bytes, err := json.Marshal(tp)
	if err != nil {
		return "", err
	} else {
		return string(bytes), nil
	}
}

func EditProperties(properties string) ([]*metapb.Column, error) {
	tp := new(TableProperty)
	log.Debug("edit properties string: %v", properties)
	if err := json.Unmarshal([]byte(properties), tp); err != nil {
		return nil, err
	}
	log.Debug("edit properties struct: %v", *tp)
	if tp.Columns == nil {
		return nil, ErrInvalidColumn
	}

	return tp.Columns, nil
}


