package metric

import (
	"sync"

	"model/pkg/statspb"
	"time"
)

type Cluster struct {
	ClusterId     uint64

	Item  *ClusterItem

	DbCache      *DbItemCache
	MacCache     *MacItemCache
	ProcessCache *ProcessItemCache

	LastReportTime time.Time
}

func NewCluster(clusterId uint64) *Cluster {
	return &Cluster {
		ClusterId: clusterId,
		Item: &ClusterItem{},
		DbCache: NewDbCache(),
		MacCache: NewMacItemCache(),
		ProcessCache: NewProcessItemCache(),
		LastReportTime: time.Now(),
	}
}

func (c *Cluster) SetMacItem(item *MacItem) {
	c.MacCache.Set(item)
}

func (c *Cluster) GetMacItem(ip string) *MacItem {
	return c.MacCache.Get(ip)
}

func (c *Cluster) SetProcessItem(item *ProcessItem) {
	c.ProcessCache.Set(item)
}

func (c *Cluster) GetProcessItem(addr string) *ProcessItem {
	return c.ProcessCache.Get(addr)
}

func (c *Cluster) SetDbItem(db *DbItem) {
	c.DbCache.Set(db)
	return
}

func (c *Cluster) GetDbItem(name string) *DbItem {
	item := c.DbCache.Get(name)
	if item != nil {
		return item
	}
	return nil
}

type ClusterItem struct {
	ClusterId     uint64     `json:"cluster_id"`
	CapacityTotal uint64     `json:"capacity_total"`
	SizeUsed      uint64     `json:"size_used"`
	RangeNum      uint64     `json:"range_num"`
	DbNum         uint64     `json:"db_num"`
	TableNum      uint64     `json:"table_num"`
	TaskNum       uint64     `json:"task_num"`
	NodeUpCount        uint64  `json:"node_up_count,omitempty"`
	NodeDownCount      uint64  `json:"node_down_count,omitempty"`
	NodeOfflineCount   uint64  `json:"node_offline_count,omitempty"`
	NodeTombstoneCount uint64  `json:"node_tombstone_count,omitempty"`
	LeaderBalanceRatio float64 `json:"leader_balance_ratio,omitempty"`
	RegionBalanceRatio float64 `json:"region_balance_ratio,omitempty"`
	GsNum         uint64     `json:"gs_num"`

	Tps           uint64     `json:"tps"`
	// 客户端请求最小延时
	Min           float64    `json:"min"`
	// 客户端请求最大延时
	Max           float64    `json:"max"`
	// 平均延时
	Avg           float64    `json:"avg"`
	// TP ...
	TP50          float64    `json:"tp50"`
	TP90          float64    `json:"tp90"`
	TP99          float64    `json:"tp99"`
	TP999         float64    `json:"tp999"`
	TotalNumber   uint64     `json:"total_number"`
	ErrNumber     uint64     `json:"err_number"`

	// 默认保留1000条 ??
	SlowLog       []string   `json:"slow_log"`

	// GS 出入口网络统计
    NetIoInBytePerSec             uint64 `json:"net_io_in_byte_per_sec"`
    NetIoOutBytePerSec            uint64 `json:"net_io_out_byte_per_sec"`
    NetIoInPackagePerSec          uint64 `json:"net_io_in_package_per_sec"`
    NetIoOutPackagePerSec         uint64 `json:"net_io_out_package_per_sec"`
    NetTcpConnections             uint32 `json:"net_tcp_connections"`
    NetTcpActiveOpensPerSec       uint32 `json:"net_tcp_activeopens_per_sec"`

	// 更新时间
	UpdateTime    int64  `json:"-"`
}

type ClusterCache struct {
	lock    sync.RWMutex
	cache   map[uint64]*Cluster
}

func NewClusterCache() *ClusterCache {
    return &ClusterCache{cache: make(map[uint64]*Cluster)}
}

func (c *ClusterCache) Set(item *Cluster) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, find := c.cache[item.ClusterId]; !find {
		c.cache[item.ClusterId] = item
	}
}

func (c *ClusterCache) Get(clusterId uint64) *Cluster {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if it, find := c.cache[clusterId]; find {
		return it
	}
	return nil
}

func (c *ClusterCache) GetAll() []*Cluster {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var items []*Cluster
	for _, item := range c.cache {
		items = append(items, item)
	}
	return items
}

type DbItem struct {
	DbName     string `json:"name"`
	TableNum   uint32 `json:"table_num"`
	Size       uint64 `json:"size"`
	UpdateTime int64  `json:"-"`

	TableCache *TableItemCache   `json:"-"`
}

func NewDbItem(name string) *DbItem {
	return &DbItem{
		DbName: name,
		TableCache: NewTableItemCache(),
	}
}

func (i *DbItem) SetTableTtem(item *TableItem) {
	i.TableCache.Set(item)
}

func (i *DbItem) GetTableItem(name string) *TableItem {
	return i.TableCache.Get(name)
}

type DbItemCache struct {
	lock    sync.RWMutex
	cache   map[string]*DbItem
}

func NewDbCache() *DbItemCache {
	return &DbItemCache{cache: make(map[string]*DbItem)}
}

func (c *DbItemCache) Set(item *DbItem) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, find := c.cache[item.DbName]; !find {
		c.cache[item.DbName] = item
	}
}

func (c *DbItemCache) Get(name string) *DbItem {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if it, find := c.cache[name]; find {
		return it
	}
	return nil
}

func (c *DbItemCache) GetAll() []*DbItem {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var items []*DbItem
	for _, item := range c.cache {
		items = append(items, item)
	}
	return items
}

type TableItem struct {
	DbName        string    `json:"db_name"`
	TableName     string    `json:"table_name"`
	RangeNum      uint64    `json:"range_num"`
	Size          uint64    `json:"size"`
	UpdateTime    int64     `json:"-"`
}

type TableItemCache struct {
	lock    sync.RWMutex
	cache   map[string]*TableItem
}

func NewTableItemCache() *TableItemCache {
	return &TableItemCache{cache: make(map[string]*TableItem)}
}

func (c *TableItemCache) Set(item *TableItem) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, find := c.cache[item.TableName]; !find {
		c.cache[item.TableName] = item
	}
}

func (c *TableItemCache) Get(name string) *TableItem {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if it, find := c.cache[name]; find {
		return it
	}
	return nil
}

func (c *TableItemCache) GetAll() []*TableItem {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var items []*TableItem
	for _, item := range c.cache {
		items = append(items, item)
	}
	return items
}

type ProcessItem struct {
	// 类型: MS, DS, GS
	Type string
	// 地址
	Addr string

	UpdateTime int64

	Item *statspb.ProcessStats

	SlowLog *statspb.SlowLogStats
}

type SlowLogItem struct {
	// 类型: MS, DS, GS
	Type string
	// 地址
	Addr string

	UpdateTime int64

	Item *statspb.SlowLogStats
}

type ProcessItemCache struct {
	lock    sync.RWMutex
	cache   map[string]*ProcessItem
}

func NewProcessItemCache() *ProcessItemCache {
	return &ProcessItemCache{cache: make(map[string]*ProcessItem)}
}

func (c *ProcessItemCache) Set(item *ProcessItem) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, find := c.cache[item.Addr]; !find {
		c.cache[item.Addr] = item
	}
}

func (c *ProcessItemCache) Get(addr string) *ProcessItem {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if it, find := c.cache[addr]; find {
		return it
	}
	return nil
}

func (c *ProcessItemCache) GetAll() []*ProcessItem{
	c.lock.RLock()
	defer c.lock.RUnlock()
	var items []*ProcessItem
	for _, item := range c.cache {
		items = append(items, item)
	}
	return items
}

type MacItem struct {
	Type string
	// Ip 地址
	Ip   string

	UpdateTime int64

    Item *statspb.MacStats
}

type MacItemCache struct {
	lock    sync.RWMutex
	cache   map[string]*MacItem
}

func NewMacItemCache() *MacItemCache {
    return &MacItemCache{cache: make(map[string]*MacItem)}
}

func (c *MacItemCache) Set(item *MacItem) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, find := c.cache[item.Ip]; !find {
		c.cache[item.Ip] = item
	}
}

func (c *MacItemCache) Get(addr string) *MacItem {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if it, find := c.cache[addr]; find {
		return it
	}
	return nil
}

func (c *MacItemCache) GetAll() []*MacItem {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var items []*MacItem
	for _, item := range c.cache {
		items = append(items, item)
	}
	return items
}

type TaskItem struct {
	UpdateTime int64
	*statspb.TaskInfo
}