package main

import (
	"os"
	"fmt"
	"strconv"
	"net"
	"log"
	"sync"
	dsClient "pkg-go/ds_client"
	"pkg-go/util"
	"runtime"
	"os/signal"
	"github.com/gogo/protobuf/proto"
	"model/pkg/kvrpcpb"
	"bufio"
	"time"
)

//启动参数： 端口 CPU数
func main() {
	address := "127.0.0.1:6680"
	cpu := 4
	var err error
	if len(os.Args) == 3 {
		address = os.Args[1]
		cpu, _ = strconv.Atoi(os.Args[2])
	} else {
		log.Fatal("please input port", err)
	}
	s := fmt.Sprintf("program run param: host: %s, cpu:%d", address, cpu)
	fmt.Println(s)
	runtime.GOMAXPROCS(cpu)
	ds := NewDsRpcServer(address)
	ds.Start()
	c := make(chan os.Signal, 1)
	signal.Notify(c) //ctrl+c : os.Interrupt, kill pid: terminated
	go func() {
		s:= <-c
		fmt.Println("exit signal:", s)
		ds.Stop()
		os.Exit(1)
	}()
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
	return &DsRpcServer{rpcAddr: addr, conns: make(map[uint64]net.Conn)}
}

func (svr *DsRpcServer) Start() {
	l, err := net.Listen("tcp", svr.rpcAddr)
	if err != nil {
		fmt.Println("listener port error:", err)
		return
	}
	fmt.Println("start to listener port :", svr.rpcAddr)
	out := new(kvrpcpb.DsKvInsertResponse)
	data, _ := proto.Marshal(out)
	svr.l = l
	svr.wg.Add(1)
	// A common pattern is to start a loop to continously accept connections
	for {
		//accept connections using Listener.Accept()
		c, err := l.Accept()
		if err != nil {
			fmt.Println("tcp accept error" +  err.Error())
			svr.wg.Done()
			return
		}
		//It's common to handle accepted connection on different goroutines
		svr.wg.Add(1)
		svr.count++
		svr.lock.Lock()
		svr.conns[svr.count] = c
		svr.lock.Unlock()
		fmt.Println("===============new connect,", svr.count)
		go svr.handleConnection(svr.count, c, data)
	}
}

func (svr *DsRpcServer) Stop() {
	if svr == nil{
	    return
	}
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

const (
	dialTimeout = 5 * time.Second
	// 128 KB
	DefaultInitialWindowSize int32 = 1024 * 64
	DefaultPoolSize          int   = 1

	// 40 KB
	DefaultWriteSize = 40960
	// 4 MB
	DefaultReadSize = 1024 * 1024 * 4
)

func (svr *DsRpcServer) handleConnection(id uint64, c net.Conn, data []byte) {
	defer func () {
		fmt.Println("================connect closed")
		c.Close()
		svr.lock.Lock()
		delete(svr.conns, id)
		svr.lock.Unlock()
		svr.wg.Done()
	}()
	writer := bufio.NewWriterSize(c, DefaultWriteSize)
	reader := bufio.NewReaderSize(c, DefaultReadSize)

	message := &dsClient.Message{}
	for {
		err := util.ReadMessage(reader, message)
		if err != nil {
			fmt.Println("+++++++++++ read message failed ", err)
			return
		}
		//fmt.Println("===========read message ", message)
		if svr.fun != nil {
			svr.fun(message)
		}
		message.SetMsgType(uint16(0x12))
		message.SetData(data)
		err = util.WriteMessage(writer, message)
		if err != nil {
			fmt.Println("+++++++++++ write message failed ", err)
			return
		}
		writer.Flush()
	}
}

