package mock

import (
	msClient "pkg-go/ms_client"
	dsClient "pkg-go/ds_client"
	"time"
	"fmt"
	"model/pkg/funcpb"
	"model/pkg/metapb"
	"net"
	"sync"
	"os"
	"model/pkg/mspb"
	"golang.org/x/net/context"
	"pkg-go/util"
	"model/pkg/kvrpcpb"
	"model/pkg/schpb"
	"github.com/gogo/protobuf/proto"
	"util/log"
	"model/pkg/taskpb"
	"runtime"
)

type Range struct {
	nodeId  uint64
	*metapb.Range
	lastHbTime time.Time
	leader *metapb.Peer
}

func NewRange(nodeId uint64, r *metapb.Range) *Range {
	index := r.GetId() % uint64(len(r.GetPeers()))
	return &Range{Range: r, leader: r.GetPeers()[index],
		lastHbTime: time.Now(), nodeId: nodeId}
}

func (r *Range) IsLeader() bool {
	if r.leader != nil {
		if r.leader.GetNodeId() == r.nodeId {
			return true
		}
	}
	return false
}

type RangeManager struct {
	lock        sync.RWMutex
	ranges      map[uint64]*Range
}

func NewRangeManager() *RangeManager{
	return &RangeManager{ranges: make(map[uint64]*Range)}
}

func (rm *RangeManager) Walk(f func(id uint64, r *Range)) {
	rm.lock.RLock()
	defer rm.lock.RUnlock()
	for k, v := range rm.ranges {
		f(k, v)
	}
}

func (rm *RangeManager) AddRange(r *Range) {
	rm.lock.Lock()
	defer rm.lock.Unlock()
	rm.ranges[r.GetId()] = r
}

func (rm *RangeManager) FindRange(id uint64) (*Range) {
	rm.lock.RLock()
	defer rm.lock.RUnlock()
	return rm.ranges[id]
}

func (rm *RangeManager) DelRange(rangeId uint64) {
	rm.lock.Lock()
	defer rm.lock.Unlock()
	delete(rm.ranges, rangeId)
}

type DsServer struct {
	*metapb.Node
	lastHbTime  time.Time
	port        uint32
	server      *DsRpcServer
	rangeManager *RangeManager
	cli         msClient.Client
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewDsServer(port uint32, msAddrs []string) *DsServer {
	os.Mkdir("/tmp/ds", 0755)
	log.InitFileLog("/tmp/ds", "debug", "debug")

	cli, err := msClient.NewClient(msAddrs)
	if err != nil {
		log.Error("new master grpc client error: %v", err)
		os.Exit(-1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ds := &DsServer{
		port: port,
		cli: cli,
		server: NewDsRpcServer(fmt.Sprintf(":%d", port)),
		rangeManager: NewRangeManager(),
		lastHbTime: time.Now(),
		ctx: ctx,
		cancel: cancel,
	}
	ds.server.fun = ds.handleMessage
	return ds
}

func (ds *DsServer) GetRangeManager() *RangeManager {
	if ds == nil {
		return nil
	}
	return ds.rangeManager

}

func (ds *DsServer) Stop() {
	ds.server.Stop()
}

func (ds *DsServer) Start() {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 1024)
			n := runtime.Stack(buf, false)
			fmt.Println("----- recover -----")
			fmt.Println(string(buf[:n]))
		}
	}()
	// getnodeid
	getNodeIdReq := &mspb.GetNodeIdRequest{
		Header: &mspb.RequestHeader{},
		ServerPort: ds.port,
		RaftPort: ds.port,
		HttpPort: ds.port,
		Version: "v1",
	}
	getNodeIdResp, err := ds.cli.GetNodeId(getNodeIdReq)
	if err != nil {
		log.Fatal("get node id error: %v", err)
	}
	//if headerError := getNodeIdResp.GetHeader().GetError(); headerError != nil {
	//	log.Error("get node id header error: %v", *headerError)
	//	os.Exit(-1)
	//}
	nodeId := getNodeIdResp.GetNodeId()
	log.Info("get node id: %v", nodeId)

	// login
	loginReq := &mspb.NodeLoginRequest{
		Header: &mspb.RequestHeader{},
		NodeId: nodeId,
	}
	if _, err := ds.cli.NodeLogin(loginReq); err != nil {
		log.Fatal("login error: %v", err)
	}
	node := &metapb.Node{
		Id:     nodeId,
		ServerAddr: fmt.Sprintf(":%d", ds.port),
		RaftAddr: fmt.Sprintf(":%d", ds.port),
		HttpAddr: fmt.Sprintf(":%d", ds.port),
		State: metapb.NodeState_N_Login,
		Version: "v1",
	}
	ds.Node = node
	go ds.server.Start()
	go ds.Heartbeat()
}

func (ds *DsServer) heartbeat() {
	fmt.Println("ds heartbeat...")
	var rangesHbReq []*mspb.RangeHeartbeatRequest
	var leaderCount, rangesCount uint32
	ds.rangeManager.Walk(func(rangeId uint64, r *Range) {
		rangesCount++
		if r.IsLeader() {
			leaderCount++
			if time.Since(r.lastHbTime) > time.Second * 10 {
				rangeHbReq := &mspb.RangeHeartbeatRequest{
					Header: &mspb.RequestHeader{},
					Range: r.Range,
					Leader: r.leader,
				}
				r.lastHbTime = time.Now()
				rangesHbReq = append(rangesHbReq, rangeHbReq)
			}
		}
	})
	nodeHbReq := &mspb.NodeHeartbeatRequest{
		Header: &mspb.RequestHeader{},
		NodeId: ds.GetId(),
		//RangeCount: rangesCount,
		//RangeLeaderCount: leaderCount,
	}
	_, err := ds.cli.NodeHeartbeat(nodeHbReq)
	if err != nil {
		fmt.Println("node heartbeat failed, err ", err)
	}
	for _, rb := range rangesHbReq {
		resp, err := ds.cli.RangeHeartbeat(rb)
		if err != nil {
			fmt.Println("range heartbeat failed, err ", err)
		}
		if resp.GetTask() != nil {
			fmt.Printf("range %d do task %s", resp.GetRangeId(), resp.GetTask().String())
			switch resp.GetTask().GetType() {
			case taskpb.TaskType_RangeDelete:
				ds.rangeManager.DelRange(resp.GetRangeId())
			}
		}
	}
}

func (ds *DsServer) handleMessage(message *dsClient.Message) {
	switch message.GetFuncId() {
	case uint16(funcpb.FunctionID_kFuncHeartbeat):

	case uint16(funcpb.FunctionID_kFuncRawGet):
		resp := kvrpcpb.DsKvRawGetResponse{
			Header: &kvrpcpb.ResponseHeader{},
			Resp: &kvrpcpb.KvRawGetResponse{
				Code: 0,
				Value: []byte("ok"),
			},
		}
		data, _ := resp.Marshal()
		message.SetData(data)
	case uint16(funcpb.FunctionID_kFuncCreateRange):
		req := new(schpb.CreateRangeRequest)
		err := proto.Unmarshal(message.GetData(), req)
		if err != nil {
			message.SetData(nil)
		} else {
			r := NewRange(ds.GetId(), req.GetRange())
			ds.rangeManager.AddRange(r)
		}
	case uint16(funcpb.FunctionID_kFuncDeleteRange):
		req := new(schpb.DeleteRangeRequest)
		err := proto.Unmarshal(message.GetData(), req)
		if err != nil {
			message.SetData(nil)
		} else {
			ds.rangeManager.DelRange(req.GetRangeId())
		}
	case uint16(funcpb.FunctionID_kFuncRangeTransferLeader):
	default:
		fmt.Printf("message: %v\n", *message)
	}
}

func (ds *DsServer) Heartbeat() {
	timer := time.NewTicker(time.Second)
	for {
		select {
		case <-ds.ctx.Done():
			return
		case <-timer.C:
			if time.Since(ds.lastHbTime) > time.Second * 10 {
				ds.lastHbTime = time.Now()
				ds.heartbeat()
			}
		}
	}
}

type WorkFunc func(msg *dsClient.Message)

type DsRpcServer struct {
	rpcAddr     string
	wg          sync.WaitGroup
	lock        sync.RWMutex
	count       uint64
	conns       map[uint64]net.Conn
	l net.Listener
	fun         WorkFunc
}

func NewDsRpcServer(addr string) *DsRpcServer {
	return &DsRpcServer{
		rpcAddr: addr,
		conns: make(map[uint64]net.Conn),
	}
}

func (svr *DsRpcServer) Start() {
	l, err := net.Listen("tcp", svr.rpcAddr)
	if err != nil {
		log.Fatal("tcp listen addr %v error: %v", svr.rpcAddr, err)
	}
	svr.l = l
	svr.wg.Add(1)
	// A common pattern is to start a loop to continuously accept connections
	for {
		//accept connections using Listener.Accept()
		c, err := l.Accept()
		if err != nil {
			svr.wg.Done()
			return
		}
		//It's common to handle accepted connection on different goroutines
		svr.wg.Add(1)
		svr.count++
		svr.lock.Lock()
		svr.conns[svr.count] = c
		svr.lock.Unlock()
		fmt.Println("===============new connect!!!!!")
		go svr.handleConnection(svr.count, c)
	}
}

func (svr *DsRpcServer) Stop() {
	if svr.l != nil {
		svr.l.Close()
	}
	svr.lock.RLock()
	for _, c := range svr.conns {
		c.Close()
	}
	svr.lock.RUnlock()
	svr.conns = make(map[uint64]net.Conn)
	svr.wg.Wait()
}

func (svr *DsRpcServer) handleConnection(id uint64, c net.Conn) {
	defer func () {
		fmt.Println("================connect closed")
		c.Close()
		svr.lock.Lock()
		delete(svr.conns, id)
		svr.lock.Unlock()
		svr.wg.Done()
	}()
	message := &dsClient.Message{}
	for {
		err := util.ReadMessage(c, message)
		if err != nil {
			return
		}
		fmt.Println("===========read message ", message)
		if svr.fun != nil {
			svr.fun(message)
		}
		message.SetMsgType(uint16(0x12))
		err = util.WriteMessage(c, message)
		if err != nil {
			fmt.Println("=============write failed ", err)
			return
		}
	}
}