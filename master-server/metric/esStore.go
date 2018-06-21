package metric

import (
	"util/log"
	"sync"
	"golang.org/x/net/context"
	"errors"
	"time"
	"github.com/olivere/elastic"
	"fmt"
)

var errInfo = "elasticsearch client is not available"

type EsStore struct {
	//es client http:port
	nodes   []string
	esClient *elastic.Client
	// 工作线程数量
	worker_num        int
	message chan *Message
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	lock sync.RWMutex
}

func NewEsStore(nodes []string, worker_num int) Store {
	ctx, cancel := context.WithCancel(context.Background())
	if worker_num <= 0 {
		worker_num = 10
	}
	if worker_num > 1000 {
		worker_num = 1000
	}
	p := &EsStore{
		nodes: nodes,
		worker_num: worker_num,
		message: make(chan *Message, 100000),
		ctx: ctx,
		cancel: cancel,
	}
	return p
}

func (s *EsStore) work() {
	log.Info("start pusher metric to elasticsearch")
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			log.Info("pusher stopped to elasticsearch: %v", s.ctx.Err())
			return
		case msg, ok := <-s.message:
			if !ok {
				continue
			}
			err := s.push(msg)
			if err != nil {
				log.Warn("push message failed, err[%v]", err)
			}
		}
	}
}

func (s *EsStore) open_() error {
	s.lock.RLock()
	if s.esClient != nil {
		s.lock.RUnlock()
		return nil
	}
	s.lock.RUnlock()

	s.lock.Lock()
	if s.esClient != nil {
		s.lock.Unlock()
		return nil
	}

	client, err := elastic.NewClient(elastic.SetURL(s.nodes...), elastic.SetSniff(false))
	if err != nil {
		log.Warn("elasticsearch client[%s] open failed, err[%v]", s.nodes, err)
		s.lock.Unlock()
		return fmt.Errorf("elastic NewClient error: %v", err)
	}
	s.esClient = client
	s.lock.Unlock()
	return nil
}

func (s *EsStore) open() error {
	err := s.open_()
	if err != nil {
		return fmt.Errorf("EsStore open error: %v", err)
	}
	return nil
}

func (s *EsStore) Open() error {
	s.wg.Add(1)
	go func () {
		defer s.wg.Done()
		timer := time.NewTicker(time.Second * 30)
		for {
			select {
			case <-s.ctx.Done():
				log.Info("pusher stopped: %v", s.ctx.Err())
				return
			case <-timer.C:
				if err := s.ping(); err != nil {
					log.Warn("elasticsearch unhealthy, err [%v]!!!!", err)
					// 重新建立链接
					_esclient, _err := elastic.NewClient(elastic.SetURL(s.nodes...), elastic.SetSniff(false))
					if _err != nil {
						log.Warn("retry connect to elastic search  [%s] failed, err[%v]", s.nodes, _err)
						continue
					}
					s.lock.Lock()
					s.esClient.Stop()
					s.esClient = _esclient
					s.lock.Unlock()
					log.Info("elastic client reconnect success")
				}
			}
		}
	}()

	for i := 0; i < s.worker_num; i++ {
		s.wg.Add(1)
		go s.work()
	}
	return nil
}

func (s *EsStore) ping() error{
	s.lock.RLock()
	if s.esClient == nil {
		s.lock.RUnlock()
		return nil
	}
	s.lock.RUnlock()
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
	defer cancel1()
	var err error
	for _, node := range s.nodes {
		_, code, err := s.esClient.Ping(node).Do(ctx1)
		if err == nil && code == 200 {
			return nil
		}
		log.Warn(" ping elasticsearch [%s] code[%d], err  [%v]!!!!", node,  code, err)
	}
	return err
}

func (s *EsStore) push(msg *Message)  error {
	num := len(msg.Items)
	if num == 0 {
		return errors.New("empty items")
	}

	if err := s.open(); err != nil {
		return fmt.Errorf("EsStore push error: %v", err)
	}

	log.Debug("start to push metric[%s] to elasticsearch.", msg.Subsystem)
	var err error
	if len(msg.MsgId) == 0 {
		err = s.insert(msg.Subsystem, msg.Subsystem, msg.Items)
		if err != nil{
			log.Warn("push metric[%s] to elasticsearch failed, err[%v]", msg.Subsystem , err)
		}
	} else {
		err = s.insertWithId(msg.Subsystem, msg.Subsystem, msg.MsgId, msg.Items)
		if err != nil{
			log.Warn("push metric[%s] to elasticsearch failed, err[%v]", msg.Subsystem , err)
		}

	}
	return err
}

func (s *EsStore) insert(index string, indexType string, items interface{} ) error{
	if s.esClient == nil {
		return errors.New(errInfo)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 10)
	defer cancel()
	_, err := s.esClient.Index().
		Index(index).
		Type(indexType).
		BodyJson(items).
		Do(ctx)
	return err
}

func (s *EsStore) insertWithId(index string, indexType string, id string, items interface{} ) error{
	if s.esClient == nil {
		return errors.New(errInfo)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 10)
	defer cancel()
	_, err := s.esClient.Index().
		Index(index).
		Type(indexType).
		Id(id).
		BodyJson(items).
		Do(ctx)
	return err
}

func (s *EsStore)Put(message *Message) error  {
	select {
	case s.message <- message:
		log.Debug("put data to es")
	default:
		log.Error("metric message queue is full!!!")
		return errors.New("message queue is full")
	}
	return nil
}

func (s *EsStore) Close() {
	s.cancel()
	s.wg.Wait()
	if s.esClient != nil {
		s.esClient.Stop()
	}
	return
}