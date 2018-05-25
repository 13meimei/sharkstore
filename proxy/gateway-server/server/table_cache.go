package server

import (
	"sync"
	"bytes"
	"time"

	"pkg-go/ms_client"
	"model/pkg/metapb"
	"util/log"
	"proxy/store/dskv"
)

type Table struct {
	*metapb.Table
	primaryKeys []string
	cli         client.Client
	deadline    time.Time

	ranges  *dskv.RangeCache
	//routes  map[uint64]*Route

	cLock     sync.RWMutex
	columns   map[string]*metapb.Column
	columnIds map[uint64]*metapb.Column
}

func NewTable(table *metapb.Table, cli client.Client, ttl time.Duration) *Table {
	t := &Table{
		Table:     table,
		cli:       cli,
		deadline:  time.Now().Add(ttl),
		ranges:    dskv.NewRangeCache(table.GetDbId(), table.GetId(), cli, dskv.NewNodeCache(cli)),
		columns:   make(map[string]*metapb.Column),
		columnIds: make(map[uint64]*metapb.Column),
	}
	log.Debug("new added Table %s.%s:", t.DbName(), t.Name())
	var pks []string
	for _, c := range table.Columns {
		log.Debug("column: {%v}", c)
		t.columns[c.Name] = c
		t.columnIds[c.Id] = c
		if c.PrimaryKey > 0 {
			pks = append(pks, c.Name)
		}
	}
	t.primaryKeys = pks
	return t
}

func (t *Table) DbName() string {
	return t.GetDbName()
}

func (t *Table) Name() string {
	return t.GetName()
}

func (t *Table) ID() uint64 {
	return t.GetId()
}

func (t *Table) PKS() []string {
	return t.primaryKeys
}

type SortRoutes []*metapb.Route

func (s SortRoutes) Len() int {
	return len(s)
}

func (s SortRoutes) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortRoutes) Less(i, j int) bool {
	if bytes.Compare(s[i].GetRange().GetStartKey(), s[j].GetRange().GetStartKey()) < 0 {
		return true
	}
	return false
}

// TODO 需要返回有序的route列表
func (t *Table) AllRoutes() []*dskv.KeyLocation {

	return nil
}

func (t *Table) FindColumn(columnName string) *metapb.Column {
	t.cLock.RLock()
	defer t.cLock.RUnlock()
	if c, ok := t.columns[columnName]; ok {
		return c
	}
	return nil
}

func (t *Table) GetAllColumns() []*metapb.Column {
	t.cLock.RLock()
	defer t.cLock.RUnlock()
	cols := make([]*metapb.Column, 0, len(t.Columns))
	for _, v := range t.Columns {
		cols = append(cols, v)
	}
	return cols
}

func (t *Table) FindColumnById(columnId uint64) *metapb.Column {
	t.cLock.RLock()
	defer t.cLock.RUnlock()
	if c, ok := t.columnIds[columnId]; ok {
		return c
	}
	return nil
}

func (t *Table) AllIndexs() []uint64 {
	indexs := make([]uint64, 0)
	t.cLock.RLock()
	defer t.cLock.RUnlock()
	for _, c := range t.columns {
		if c.Index {
			indexs = append(indexs, c.Id)
		}
	}
	return indexs
}

func (t *Table) AddColumn(c *metapb.Column) {
	if c == nil {
		return
	}

	t.cLock.Lock()
	defer t.cLock.Lock()
	t.columns[c.Name] = c
	t.columnIds[c.Id] = c
}

func (t *Table) DeleteColumn(columnName string) {
	t.cLock.Lock()
	defer t.cLock.Lock()
	if c, ok := t.columns[columnName]; ok {
		delete(t.columns, columnName)
		delete(t.columnIds, c.Id)
	}

	return
}

func (t *Table) PkDupCheck() bool {
	return t.GetPkDupCheck()
}
