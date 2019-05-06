package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"master-server/engine/boltstore"
	sErr "master-server/engine/errors"
	"master-server/engine/model"
	"master-server/raft"
	raftproto "master-server/raft/proto"
	"master-server/raft/storage/wal"
	"master-server/raftgroup"
	"model/pkg/ms_raftcmdpb"
	"util/log"
)

const (
	SUCCESS            int32 = 0
	ER_NOT_LEADER            = 1
	ER_SERVER_BUSY           = 2
	ER_SERVER_STOP           = 3
	ER_READ_ONLY             = 4
	ER_ENTITY_NOT_EXIT       = 5
	ER_UNKNOWN               = 6

	// SQL ERROR CODE from 1000
)

var DefaultRaftLogCount uint64 = 10000
var ErrUnknownCommandType = errors.New("unknown command type")
var DefaultMaxSubmitTimeout time.Duration = time.Second * 60

type Iterator interface {
	// return false if over or error
	Next() bool

	Key() []byte
	Value() []byte

	Error() error

	// Release iterator使用完需要释放
	Release()
}

type Store interface {
	Open() error
	Put(key, value []byte) error
	Delete(key []byte) error
	Get(key []byte) ([]byte, error)
	Scan(startKey, limitKey []byte) Iterator
	NewBatch() Batch
	Close() error
}

type Batch interface {
	Put(key []byte, value []byte)
	Delete(key []byte)

	Commit() error
}

//{"id":1,"ip":"127.0.165.52", "http_port":8887,"rpc_port":8888, "raft_ports":[8877,8867]}
type Member struct {
	Id        uint64   `json:"id"`
	Ip        string   `json:"ip"`
	HttpPort  uint16   `json:"http_port"`
	RpcPort   uint16   `json:"rpc_port"`
	RaftPorts []uint16 `json:"raft_ports"`
}

type Peer struct {
	ID                uint64 `json:"id"`
	WebManageAddr     string `json:"web_addr"`
	RpcServerAddr     string `json:"rpc_addr"`
	RaftHeartbeatAddr string `json:"raft_hb_addr"`
	RaftReplicateAddr string `json:"raft_rp_addr"`
}

func (p *Peer) GetId() uint64 {
	if p == nil {
		return 0
	}
	return p.ID
}

type StoreConfig struct {
	RaftRetainLogs        int64
	RaftHeartbeatInterval time.Duration
	RaftHeartbeatAddr     string
	RaftReplicateAddr     string
	RaftPeers             []*Peer

	NodeID   uint64
	DataPath string

	LeaderChangeHandler raftgroup.RaftLeaderChangeHandler
	FatalHandler        raftgroup.RaftFatalEventHandler
}

type RaftStore struct {
	dataPath   string
	store      model.Store
	raft       *raftgroup.RaftGroup
	raftServer *raft.RaftServer
	raftConfig *raft.RaftConfig
	localRead  bool
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func NewRaftStore(conf *StoreConfig) (*RaftStore, error) {
	store := &RaftStore{}
	var raftPeers []raftproto.Peer
	var nodes map[uint64]*Peer
	nodes = make(map[uint64]*Peer)
	raftPeers = make([]raftproto.Peer, 0, len(conf.RaftPeers))
	for _, p := range conf.RaftPeers {
		peer := raftproto.Peer{Type: raftproto.PeerNormal, ID: p.ID}
		raftPeers = append(raftPeers, peer)
		nodes[p.ID] = p
	}
	rc := raft.DefaultConfig()
	rc.RetainLogs = uint64(conf.RaftRetainLogs)
	rc.TickInterval = conf.RaftHeartbeatInterval
	rc.HeartbeatAddr = conf.RaftHeartbeatAddr
	rc.ReplicateAddr = conf.RaftReplicateAddr
	// master server cluster
	rc.Resolver = NewResolver(nodes)
	rc.NodeID = uint64(conf.NodeID)
	rs, err := raft.NewRaftServer(rc)
	if err != nil {
		log.Error("new raft server failed, err[%v]", err)
		return nil, err
	}

	raftGroup := raftgroup.NewRaftGroup(1, rs, []byte("\x00"), []byte("\xff"))
	path := filepath.Join(conf.DataPath, "data")
	err = os.MkdirAll(path, 0755)
	if err != nil {
		log.Error("make dir %s failed, err[%v]", path, err)
		return nil, err
	}
	path = filepath.Join(path, "fbase.db")
	rowStore, applyId, err := boltstore.NewBoltStore(path)
	if err != nil {
		log.Error("open store failed, err[%v]", err)
		return nil, err
	}
	// raft group create at end !!!!!!!
	path = filepath.Join(conf.DataPath, "raft")
	raftStorage, err := wal.NewStorage(1, path, nil)
	if err != nil {
		log.Error("service new raft store error: %s", err)
		return nil, err
	}

	raftGroup.RegisterApplyHandle(store.HandleCmd)
	raftGroup.RegisterPeerChangeHandle(store.HandlePeerChange)
	raftGroup.RegisterGetSnapshotHandle(store.HandleGetSnapshot)
	raftGroup.RegisterApplySnapshotHandle(store.HandleApplySnapshot)
	raftGroup.RegisterLeaderChangeHandle(conf.LeaderChangeHandler)
	raftGroup.RegisterFatalEventHandle(conf.FatalHandler)

	var raftConfig *raft.RaftConfig
	raftConfig = &raft.RaftConfig{
		ID:           1,
		Applied:      applyId,
		Peers:        raftPeers,
		Storage:      raftStorage,
		StateMachine: raftGroup,
	}
	store.ctx, store.cancel = context.WithCancel(context.Background())
	store.store = rowStore
	store.raft = raftGroup
	store.raftServer = rs
	store.raftConfig = raftConfig
	store.localRead = true
	store.dataPath = conf.DataPath
	return store, nil
}

func (s *RaftStore) raftLogCleanup() {
	defer s.wg.Done()
	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						switch x := r.(type) {
						case string:
							fmt.Printf("Error: %s.\n", x)
						case error:
							fmt.Printf("Error: %s.\n", x.Error())
						default:
							fmt.Printf("Unknown panic error.%v", r)
						}
					}
				}()
				applyId := s.store.Applied()
				if applyId < DefaultRaftLogCount {
					return
				}
				s.raftServer.Truncate(1, applyId-DefaultRaftLogCount)
			}()
		}
	}
}

func (s *RaftStore) Open() error {
	err := s.raft.Create(s.raftConfig)
	if err != nil {
		return err
	}
	s.wg.Add(1)
	go s.raftLogCleanup()
	return nil
}

func (s *RaftStore) Put(key, value []byte) error {
	req := &ms_raftcmdpb.Request{
		CmdType: ms_raftcmdpb.CmdType_Put,
		PutReq: &ms_raftcmdpb.PutRequest{
			Key:   key,
			Value: value,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), DefaultMaxSubmitTimeout)
	defer cancel()
	_, err := s.raft.SubmitCommand(ctx, req)
	if err != nil {
		// TODO error
		log.Error("raft submit failed, err[%v]", err.Error())
		return err
	}
	return nil
}

func (s *RaftStore) Delete(key []byte) error {
	req := &ms_raftcmdpb.Request{
		CmdType: ms_raftcmdpb.CmdType_Delete,
		DeleteReq: &ms_raftcmdpb.DeleteRequest{
			Key: key,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), DefaultMaxSubmitTimeout)
	defer cancel()
	_, err := s.raft.SubmitCommand(ctx, req)
	if err != nil {
		// TODO error
		log.Error("raft submit failed, err[%v]", err.Error())
		return err
	}
	return nil
}

func (s *RaftStore) Get(key []byte) ([]byte, error) {
	if s.localRead {
		return s.store.Get(key)
	}
	req := &ms_raftcmdpb.Request{
		CmdType: ms_raftcmdpb.CmdType_Get,
		GetReq: &ms_raftcmdpb.GetRequest{
			Key: key,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), DefaultMaxSubmitTimeout)
	defer cancel()
	resp, err := s.raft.SubmitCommand(ctx, req)
	if err != nil {
		return nil, err
	}
	value := resp.GetGetResp().GetValue()
	return value, nil
}

func (s *RaftStore) Scan(startKey, limitKey []byte) Iterator {
	return s.store.NewIterator(startKey, limitKey)
}

func (s *RaftStore) NewBatch() Batch {
	return NewSaveBatch(s.raft)
}

func (s *RaftStore) Close() error {
	s.raft.Release()
	s.cancel()
	s.wg.Wait()
	return s.store.Close()
}

func (s *RaftStore) raftKvRawGet(req *ms_raftcmdpb.GetRequest, raftIndex uint64) (*ms_raftcmdpb.GetResponse, error) {
	resp := new(ms_raftcmdpb.GetResponse)
	//log.Info("raft put")
	// TODO write in one batch
	value, err := s.store.Get(req.GetKey())
	if err != nil {
		if err == sErr.ErrNotFound {
			resp.Code = SUCCESS
			resp.Value = nil
			return resp, nil
		}
		return nil, err
	}
	resp.Code = SUCCESS
	resp.Value = value
	return resp, nil
}

func (s *RaftStore) raftKvRawPut(req *ms_raftcmdpb.PutRequest, raftIndex uint64) (*ms_raftcmdpb.PutResponse, error) {
	resp := new(ms_raftcmdpb.PutResponse)
	// TODO write in one batch
	err := s.store.Put(req.GetKey(), req.GetValue(), 0, raftIndex)
	if err != nil {
		return nil, err
	}
	resp.Code = SUCCESS
	return resp, nil
}

func (s *RaftStore) raftKvRawDelete(req *ms_raftcmdpb.DeleteRequest, raftIndex uint64) (*ms_raftcmdpb.DeleteResponse, error) {
	resp := new(ms_raftcmdpb.DeleteResponse)
	err := s.store.Delete(req.GetKey(), raftIndex)
	if err != nil {
		return nil, err
	}
	resp.Code = SUCCESS
	return resp, nil
}

func (s *RaftStore) raftKvRawExecute(req *ms_raftcmdpb.ExecuteRequest, raftIndex uint64) (*ms_raftcmdpb.ExecuteResponse, error) {
	resp := new(ms_raftcmdpb.ExecuteResponse)
	batch := s.store.NewWriteBatch()
	for _, e := range req.GetExecs() {
		switch e.Do {
		case ms_raftcmdpb.ExecuteType_ExecPut:
			batch.Put(e.KvPair.Key, e.KvPair.Value, 0, raftIndex)
		case ms_raftcmdpb.ExecuteType_ExecDelete:
			batch.Delete(e.KvPair.Key, raftIndex)
		}
	}
	err := batch.Commit()
	if err != nil {
		return nil, err
	}
	resp.Code = SUCCESS
	return resp, nil
}

func (s *RaftStore) HandleCmd(req *ms_raftcmdpb.Request, raftIndex uint64) (resp *ms_raftcmdpb.Response, err error) {
	resp = new(ms_raftcmdpb.Response)
	resp.CmdType = req.GetCmdType()

	// TODO check split status
	switch req.GetCmdType() {
	case ms_raftcmdpb.CmdType_Get:
		_resp, err := s.raftKvRawGet(req.GetGetReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.GetResp = _resp
	case ms_raftcmdpb.CmdType_Put:
		_resp, err := s.raftKvRawPut(req.GetPutReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.PutResp = _resp
	case ms_raftcmdpb.CmdType_Delete:
		_resp, err := s.raftKvRawDelete(req.GetDeleteReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.DeleteResp = _resp
	case ms_raftcmdpb.CmdType_Execute:
		_resp, err := s.raftKvRawExecute(req.GetExecuteReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.ExecuteResp = _resp
	}
	return resp, nil
}

func (s *RaftStore) GetSnapshot() (model.Snapshot, error) {
	return s.store.GetSnapshot()
}

func (s *RaftStore) ApplySnapshot(iter *raftgroup.SnapshotKVIterator) error {
	var err error
	var pair *ms_raftcmdpb.RaftKvPair
	// TODO clear store and reopen store
	dirPath := filepath.Join(s.dataPath, "data")
	pathTemp := filepath.Join(dirPath, fmt.Sprintf("fbase.db.%s", time.Now().Format(time.RFC3339Nano)))
	path := filepath.Join(dirPath, "fbase.db")
	newStore, _, err := boltstore.NewBoltStore(pathTemp)
	if err != nil {
		log.Error("open new store failed, err[%v]", err)
		os.Remove(pathTemp)
		return err
	}
	for {
		pair, err = iter.Next()
		if err == io.EOF { // EOF代表数据传输完成
			err = nil
			log.Info("apply snapshot finished.")
			break
		} else if err == nil { // 取数据没出错，应用到store
			err = newStore.Put(pair.Key, pair.Value, 0, pair.ApplyIndex)
		}

		// 同时处理Next和Put的错误
		if err != nil {
			log.Error("apply snapshot error [%v]", err)
			newStore.Close()
			os.Remove(pathTemp)
			return err
		}
	}
	// 应用成功了
	newStore.Close()
	s.store.Close()
	// 替换文件
	err = os.Rename(pathTemp, path)
	if err != nil {
		log.Fatal("rename db file failed, err %v", err)
	}
	newStore, _, err = boltstore.NewBoltStore(path)
	if err != nil {
		log.Fatal("reopen store failed, err[%v]", err)
	}
	s.store = newStore
	log.Info("apply snapshot end")
	return nil
}

func (s *RaftStore) HandlePeerChange(confChange *raftproto.ConfChange) (res interface{}, err error) {
	switch confChange.Type {
	case raftproto.ConfAddNode:

		res, err = nil, nil
	case raftproto.ConfRemoveNode:

		res, err = nil, nil
	case raftproto.ConfUpdateNode:
		log.Debug("update range peer")
		res, err = nil, nil
	default:
		res, err = nil, ErrUnknownCommandType
	}

	return
}

////TODO
func (s *RaftStore) HandleGetSnapshot() (model.Snapshot, error) {
	log.Info("get snapshot")
	return s.GetSnapshot()
}

func (s *RaftStore) HandleApplySnapshot(peers []raftproto.Peer, iter *raftgroup.SnapshotKVIterator) error {
	log.Info("apply snapshot")
	return s.ApplySnapshot(iter)
}

type SaveBatch struct {
	raft  *raftgroup.RaftGroup
	lock  sync.RWMutex
	batch []*ms_raftcmdpb.KvPairExecute
}

func NewSaveBatch(raft *raftgroup.RaftGroup) *SaveBatch {
	return &SaveBatch{raft: raft, batch: nil}
}

func (b *SaveBatch) Put(key, value []byte) {
	_key := make([]byte, len(key))
	_value := make([]byte, len(value))
	copy(_key, key)
	copy(_value, value)
	exec := &ms_raftcmdpb.KvPairExecute{
		Do:     ms_raftcmdpb.ExecuteType_ExecPut,
		KvPair: &ms_raftcmdpb.KvPair{Key: _key, Value: _value},
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	b.batch = append(b.batch, exec)
}

func (b *SaveBatch) Delete(key []byte) {
	_key := make([]byte, len(key))
	copy(_key, key)
	exec := &ms_raftcmdpb.KvPairExecute{
		Do:     ms_raftcmdpb.ExecuteType_ExecDelete,
		KvPair: &ms_raftcmdpb.KvPair{Key: _key},
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	b.batch = append(b.batch, exec)
}

func (b *SaveBatch) Commit() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	batch := b.batch
	b.batch = nil
	// 空提交
	if len(batch) == 0 {
		return nil
	}
	req := &ms_raftcmdpb.Request{
		CmdType: ms_raftcmdpb.CmdType_Execute,
		ExecuteReq: &ms_raftcmdpb.ExecuteRequest{
			Execs: batch,
		},
	}
	_, err := b.raft.SubmitCommand(context.Background(), req)
	if err != nil {
		// TODO error
		log.Error("raft submit failed, err[%v]", err.Error())
		return err
	}
	return nil
}

type Resolver struct {
	cluster map[uint64]*Peer
}

func NewResolver(nodes map[uint64]*Peer) *Resolver {
	return &Resolver{cluster: nodes}
}

func (r *Resolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	switch stype {
	case raft.HeartBeat:
		node := r.cluster[nodeID]
		if node == nil {
			return "", errors.New("invalid node")
		}
		return node.RaftHeartbeatAddr, nil
	case raft.Replicate:
		node := r.cluster[nodeID]
		if node == nil {
			return "", errors.New("invalid node")
		}
		return node.RaftReplicateAddr, nil
	default:
		return "", errors.New("unknown socket type")
	}

	return "", nil
}
