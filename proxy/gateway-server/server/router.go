package server

import (
	"sync"
	"time"

	"model/pkg/metapb"
	"util/log"
	"pkg-go/ms_client"
	"util/ttlcache"
)

type Router struct {
	cli     client.Client
	lock    sync.RWMutex
	dbNs     map[string]*DataBase
	dbIs     map[uint64]*DataBase

	missDbs *ttlcache.TTLCache

	quit    chan struct{}
}

func NewRouter(cli client.Client) *Router {
	router := &Router{
		cli:     cli,
		dbNs:     make(map[string]*DataBase),
		dbIs:     make(map[uint64]*DataBase),
		missDbs: ttlcache.NewTTLCache(time.Second * 60),
		quit: make(chan struct{}),
	}
	return router
}

func (rr *Router) findDB(dbname string) *DataBase {
	rr.lock.RLock()
	defer rr.lock.RUnlock()
	if db, ok := rr.dbNs[dbname]; ok {
		return db
	}
	return nil
}

func (rr *Router) findDBById(dbId uint64) *DataBase {
	rr.lock.RLock()
	defer rr.lock.RUnlock()
	if db, ok := rr.dbIs[dbId]; ok {
		return db
	}
	return nil
}

func (rr *Router) addColumnToRemote(dbId,tableId uint64,cols []string) error{
	columns := make([]*metapb.Column, 0)
	for _,name := range cols {
		col := &metapb.Column{
			Name:name,
		}
		columns = append(columns,col)
	}
	respCols ,err := rr.cli.AddColumns(dbId,tableId,columns)
	if err != nil {
		return err
	}
	if log.GetFileLogger().IsEnableDebug() {
		log.Debug("request size:%d,response size:%d,%v",len(cols),len(respCols),respCols)
	}
	return nil
}

func (rr *Router) findDbFromRemote(dbname string) *DataBase {
	var _db *metapb.DataBase
	var err error
	retry := 10
	for i := 0; i < retry; i++ {
		_db, err = rr.cli.GetDB(dbname)
		if err != nil {
			time.Sleep(time.Millisecond * time.Duration(10 * (i + 1)))
			continue
		} else {
			break
		}
	}
	if err != nil {
		log.Error("get db[%s] from master server failed, err[%v]", dbname, err)
		return nil
	}
	return NewDataBase(_db, rr.cli)
}

func (rr *Router) FindDB(dbname string) *DataBase {
	db := rr.findDB(dbname)
	if db == nil {
		// 查询miss缓存，如果存在miss，则直接返回
		_, find := rr.missDbs.Get(dbname)
		if find {
			return nil
		}
		db = rr.findDbFromRemote(dbname)
		if db == nil {
			// 缓存到miss cache
			rr.missDbs.Put(dbname, dbname)
			return nil
		}
		rr.lock.Lock()
		defer rr.lock.Unlock()
		if _db, find := rr.dbNs[dbname]; find {
			return _db
		}
		rr.dbNs[dbname] = db
		rr.dbIs[db.GetId()] = db
		rr.missDbs.Delete(dbname)
	}
	return db
}

func (rr *Router) FindTable(dbName, tableName string) *Table {
	db := rr.FindDB(dbName)
	if db != nil {
		return db.FindTable(tableName)
	}
	return nil
}

func (rr *Router) FindColumn(db, table, clo string) *metapb.Column {
	t := rr.FindTable(db, table)
	if t != nil {
		return t.FindColumn(clo)
	}
	return nil
}

func (rr *Router) GetAllColumns(db, table string) []*metapb.Column {
	t := rr.FindTable(db, table)
	if t != nil {
		return t.GetAllColumns()
	}
	return nil
}