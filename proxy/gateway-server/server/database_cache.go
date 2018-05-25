package server

import (
	"sync"
	"time"

	"pkg-go/ms_client"
	"model/pkg/metapb"
	"util/log"
	"util/ttlcache"
)

type DataBase struct {
	*metapb.DataBase
	// more ......
	cli    client.Client
	lock   sync.RWMutex
	tables map[string]*Table

	missTables *ttlcache.TTLCache
}

func NewDataBase(db *metapb.DataBase, cli client.Client) *DataBase {
	return &DataBase{
		DataBase:      db,
		cli:           cli,
		tables:        make(map[string]*Table),
		missTables:    ttlcache.NewTTLCache(time.Second * 60),
	}
}

func (d *DataBase) DbName() string {
	return d.GetName()
}

func (d *DataBase) DbId() uint64 {
	return d.GetId()
}

func (d *DataBase) findTable(tableName string) *Table {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if t, ok := d.tables[tableName]; ok {
		return t
	}
	return nil
}

func (d *DataBase) loadTableFromRemote(tableName string) (*metapb.Table, error) {
	t, err := d.cli.GetTable(d.DbName(), tableName)
	if err != nil {
		log.Error("find table from remote failed, err[%v]", err)
		return nil, err
	}
	return t, nil
}

func (d *DataBase) loadTableFromRemoteById(tableId uint64) (*metapb.Table, error) {
	t, err := d.cli.GetTableById(d.DbId(), tableId)
	if err != nil {
		log.Error("find table from remote failed, err[%v]", err)
		return nil, err
	}
	return t, nil
}

func (d *DataBase) FindTable(tableName string) *Table {
	t := d.findTable(tableName)
	if t == nil {
		d.lock.Lock()
		defer d.lock.Unlock()
		if t, ok := d.tables[tableName]; ok {
			return t
		}
		// 查询miss cache
		_, find := d.missTables.Get(tableName)
		if find {
			return nil
		}
		// TODO table not exist and delete or other error
		_t, err := d.loadTableFromRemote(tableName)
		if err != nil {
			log.Warn("load table failed, err[%v]", err)
			return nil
		}
		if _t == nil {
			d.missTables.Put(tableName, tableName)
			return nil
		}
		t = NewTable(_t, d.cli, 5 * time.Minute)
		d.tables[t.Name()] = t
		d.missTables.Delete(t.Name())
	}
	// 存储时间到，确认是否继续保留缓存
	if t.deadline.Before(time.Now()) {
		d.lock.Lock()
		defer d.lock.Unlock()
		if t, ok := d.tables[tableName]; ok {
			if t.deadline.After(time.Now()) {
				return t
			}
		}
		_t, err := d.loadTableFromRemote(tableName)
		if err != nil {
			log.Warn("load table failed, err[%v]", err)
			return t
		}
		// table已经被删除
		if _t == nil {
			delete(d.tables, t.GetName())
			return nil
		}
		table := NewTable(_t, d.cli, 5 * time.Minute)
		if t.GetId() == _t.GetId() {
			table.ranges = t.ranges
		}
		d.tables[table.Name()] = table
		d.missTables.Delete(table.Name())
		return table
	}
	return t
}

func (d *DataBase) AddTable(t *Table) {
	if t == nil {
		return
	}
	d.lock.Lock()
	defer d.lock.Unlock()
	d.tables[t.Name()] = t
	d.missTables.Delete(t.Name())
	//go d.routesUpdateLoop(t)
}