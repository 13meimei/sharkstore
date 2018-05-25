package mock_ms

import (
	"sync"

	"model/pkg/metapb"
)

type Database struct {
	*metapb.DataBase
	lock    sync.Mutex
	// 存放当前的table
	tables *TableCache
}

func NewDatabase(db *metapb.DataBase) *Database {
	return &Database{
		DataBase: db,
		tables: NewTableCache(),
	}
}

func (db *Database) Name() string {
	return db.GetName()
}

// 查找当前的table
func (db *Database) FindTable(name string) (*Table, bool) {
	return db.tables.FindTableByName(name)
}

// 查找存在的table
func (db *Database) FindTableById(id uint64) (*Table, bool) {
	return db.tables.FindTableById(id)
}

func (db *Database) Lock() {
	db.lock.Lock()
}

func (db *Database) UnLock() {
	db.lock.Unlock()
}

func (db *Database) AddTable(t *Table) {
	db.tables.Add(t)
}

// real delete
func (db *Database) DeleteTableById(id uint64) error {
	_, find := db.tables.FindTableById(id)
	if !find {
		return ErrNotExistTable
	}
	db.tables.DeleteById(id)
	return nil
}

// 仅仅从当前table列表中删除
func (db *Database) DeleteTableByName(name string) error {
	db.tables.DeleteByName(name)
	return nil
}

func (db *Database) GetAllTable() []*Table {
	return db.tables.GetAllTable()
}

type DbCache struct {
	lock    sync.RWMutex
	dbNs     map[string]*Database
	dbIs     map[uint64]*Database
}

func NewDbCache() *DbCache {
	return &DbCache{
		dbNs: make(map[string]*Database),
		dbIs: make(map[uint64]*Database)}
}

func (dc *DbCache) Add(d *Database) {
	dc.lock.Lock()
	defer dc.lock.Unlock()
	dc.dbNs[d.Name()] = d
	dc.dbIs[d.GetId()] = d
}

func (dc *DbCache) Delete(name string) {
	dc.lock.Lock()
	defer dc.lock.Unlock()
	if d, find := dc.dbNs[name]; find {
		delete(dc.dbNs, name)
		delete(dc.dbIs, d.GetId())
		return
	}
}

func (dc *DbCache) FindDb(name string) (*Database, bool) {
	dc.lock.RLock()
	defer dc.lock.RUnlock()
	if d, find := dc.dbNs[name]; find {
		return d, true
	}
	return nil, false
}

func (dc *DbCache) FindDbById(id uint64) (*Database, bool) {
	dc.lock.RLock()
	defer dc.lock.RUnlock()
	if d, find := dc.dbIs[id]; find {
		return d, true
	}
	return nil, false
}

func (dc *DbCache) GetAllDatabase() []*Database {
	dc.lock.RLock()
	defer dc.lock.RUnlock()
	var dbs []*Database
	for _, d := range dc.dbNs {
		dbs = append(dbs, d)
	}
	return dbs
}

func (dc *DbCache) Size() int {
	dc.lock.RLock()
	defer dc.lock.RUnlock()
	return len(dc.dbNs)
}
