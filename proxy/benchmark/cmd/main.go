package main

import (
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"model/pkg/metapb"
	"proxy/benchmark/blob_store"
	"proxy/gateway-server/server"
	"sync"
	"util"
	"util/gogc"
	"util/log"
)

var (
	configFileName = flag.String("config", "", "Usage : -config conf/config.toml")

	TableFields = []string{"h", "user_name", "pass_word", "real_name"}
	HField      = []string{"h"}
	UField      = []string{"user_name"}
	PField      = []string{"pass_word"}

	tableId uint64 = 3
	//tableId uint64 = 209

	//blob db leavel
	path = "./data/blob_dir"
)

var cfg *Config
var api *server.SharkStoreApi
var stat *Stat

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU() - 1)

	// load bench mark config file
	cfg = new(Config)
	cfg.LoadConfig(configFileName)

	// init logger
	log.InitFileLog(cfg.Log.Dir, cfg.Log.Module, cfg.Log.Level)
	log.Info("bench config: %v", *cfg)

	// create global api
	proxyCfg := &server.ProxyConfig{
		AggrEnable:  cfg.AggrEnable,
		MaxLimit:    cfg.MaxLimit,
		Performance: cfg.Performance,
	}
	api = server.NewSharkStoreAPI(cfg.Cluster.ServerAddr, proxyCfg)

	go benchmark()
	gogc.TickerPrintGCSummary(log.GetFileLogger(), "info")
}

/**

*for benchmark
 */

//=================================================================

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func makeString(length int) string {
	var result []byte
	for i := 0; i < length; i++ {
		result = append(result, byte('a'))
	}
	return string(result)
}

func randomString(length int) string {
	str := "!@#$^&*()_+<>?:{}|;.,/][-=0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < length; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}

type Stat struct {
	lastCount int64
	preCount  int64
	errCount  int64

	lastSelCount int64
	preSelCount  int64
	errSelCount  int64

	lastUpdCount int64
	preUpdCount  int64
	errUpdCount  int64

	delCount int64
}

func benchmark() {
	stat = &Stat{}
	time.Sleep(1000)

	go func() {
		defer func() {
			log.Error("cal err ")
			if err := recover(); err != nil {
				log.Error("%v", err)
			}
		}()
		ticker := time.Tick(1 * time.Second)

		preTime := time.Now().UnixNano()
		for now := range ticker {
			total := atomic.LoadInt64(&stat.lastCount)
			selTotal := atomic.LoadInt64(&stat.lastSelCount)
			updTotal := atomic.LoadInt64(&stat.lastUpdCount)
			log.Error("%s: total:%d, ops:%d, err:%d \n sel: total:%d, ops:%d,err:%d \n upd: total:%d, ops:%d, err:%d \n  del: total:%d",
				time.Now().Format("2006-01-02T15:04:05"),
				total, (total-stat.preCount)*1000*1000*1000/(now.UnixNano()-preTime), stat.errCount,
				selTotal, (selTotal-stat.preSelCount)*1000*1000*1000/(now.UnixNano()-preTime), stat.errSelCount,
				updTotal, (updTotal-stat.preUpdCount)*1000*1000*1000/(now.UnixNano()-preTime), stat.errUpdCount,
				stat.delCount)
			stat.preCount = total
			stat.preSelCount = selTotal
			stat.preUpdCount = updTotal
			preTime = now.UnixNano()
		}
	}()
	ip := getIp()

	//insert data
	if cfg.BenchConfig.Type == 1 {
		for concur := 0; concur < cfg.BenchConfig.Threads; concur++ {
			go insertTestData(concur, 10000, ip)
		}
	}

	//insert data with auto increment pk
	if cfg.BenchConfig.Type == 2 {
		for concur := 0; concur < cfg.BenchConfig.Threads; concur++ {
			go insertNoPkTestData(concur, 10000, ip)
		}
	}

	//select data
	if cfg.BenchConfig.Type == 3 {
		for concur := 0; concur < cfg.BenchConfig.Threads; concur++ {
			go selectTest(concur, 10000, ip)
		}
	}

	//correct and concurrent check update function
	if cfg.BenchConfig.Type == 4 {
		// when select, check elapsed time and correctness after updating
		go correctCheck4Update()
	}

	if cfg.BenchConfig.Type == 5 {
		// when select, check elapsed time and correctness after deleting
		// when select, check elapsed time and correctness after inserting
		go correctCheck4DelAndInsert()
	}

	//check the elapsed time and correctness after deleting at blob_db level
	if cfg.BenchConfig.Type == 6 {
		go correct4BatchDelete()
	}

	// order insert
	if cfg.BenchConfig.Type == 10 {
		for concur := 0; concur < cfg.BenchConfig.Threads; concur++ {
			go insertOrder(concur, 0, ip)
		}
	}
	// order select
	if cfg.BenchConfig.Type == 11 {
		for concur := 0; concur < cfg.BenchConfig.Threads; concur++ {
			go selectOrder(concur, 0, ip)
		}
	}
	// order insert and select
	if cfg.BenchConfig.Type == 12 {
		var wg sync.WaitGroup
		for concur := 0; concur < cfg.BenchConfig.Threads; concur++ {
			wg.Add(1)
			func(wg *sync.WaitGroup) {
				defer wg.Done()
				insertOrder(concur, 0, ip)
			}(&wg)
		}
		wg.Wait()

		for concur := 0; concur < cfg.BenchConfig.Threads; concur++ {
			wg.Add(1)
			func(wg *sync.WaitGroup) {
				defer wg.Done()
				selectOrder(concur, 0, ip)
			}(&wg)
		}
		wg.Wait()
	}
	// raw set
	if cfg.BenchConfig.Type == 13 {
		for concur := 0; concur < cfg.BenchConfig.Threads; concur++ {
			go rawSet(concur, ip)
		}
	}
	// raw get
	if cfg.BenchConfig.Type == 14 {
		for concur := 0; concur < cfg.BenchConfig.Threads; concur++ {
			go rawGet(concur, ip)
		}
	}
}

func getIp() string {
	ip := "127.0.0.1"
	adders, err := net.InterfaceAddrs()
	if err == nil {
		for _, addr := range adders {
			curIp := addr.String()
			if strings.HasPrefix(curIp, "172") || strings.HasPrefix(curIp, "10") || strings.HasPrefix(curIp, "11") {
				ip = curIp
				break
			}
		}
	}
	return ip
}

func selectTest(threadNo, total int, ip string) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	pks := make(map[string]interface{})
	for {
		no := r.Intn(cfg.BenchConfig.SendNum)
		user_name := getUserName(no, ip, threadNo)
		h := hash(user_name) % 16384
		pks["user_name"] = user_name
		pks["h"] = h
		reply := api.Select(cfg.BenchConfig.DB, cfg.BenchConfig.Table, TableFields, pks, nil)
		log.Debug("userName %s, h %v, select result:%v", user_name, h, reply)
		if reply.Code == 0 && len(reply.Values) > 0 {
			atomic.AddInt64(&stat.lastSelCount, 1)
		} else {
			atomic.AddInt64(&stat.errSelCount, 1)
			if reply.Code == 0 {
				log.Debug("%v", reply)
			} else {
				log.Warn("execute failed, %v", reply)
			}

		}
	}

}

// check correct: select after update
func correctCheck4Update() {
	if cfg.BenchConfig.Scope < 1 || cfg.BenchConfig.Scope > 16384 {
		log.Fatal("bench config scope should be between 1 and 16384")
	}
	updateMsg := fmt.Sprintf("update message, %v", time.Now().Format("2006-01-02 15:04:05.000"))
	for i := 1; i <= cfg.BenchConfig.Scope; i++ {
		h := uint32(i - 1)
		userNames, err := selectSource(h)
		if err != nil || len(userNames) == 0 {
			log.Fatal("h %v no user", h)
		}
		log.Info("h: %v source data length: %v", h, len(userNames))
		threadNum := cfg.BenchConfig.Threads
		increase := len(userNames)/threadNum + 1
		for concur := 0; concur < threadNum; concur++ {
			start := concur * increase
			end := start + increase
			if start >= len(userNames) {
				break
			}
			if end > len(userNames) {
				end = len(userNames)
			}
			go func() {
				log.Debug("h: %v update data scope between %v and %v", h, start, end)
				subUserNames := userNames[start:end]
				rows := make([][]interface{}, 0)
				keyArray := make([]string, 0)
				var loop int
				for i := 0; i < len(subUserNames); i++ {
					if cfg.BenchConfig.Batch > 1 {
						if loop == cfg.BenchConfig.Batch {
							log.Info("h: %v start %v end %v, size: %v", h, start, end, len(keyArray))
							for {
								reply := api.Insert(cfg.BenchConfig.DB, cfg.BenchConfig.Table, TableFields, rows)
								if reply.Code == 0 {
									atomic.AddInt64(&stat.lastUpdCount, 1)
									checkElapsedTimeBatch(h, keyArray, updateMsg)
									rows = make([][]interface{}, 0)
									keyArray = make([]string, 0)
									loop = 0
									break
								} else {
									atomic.AddInt64(&stat.errUpdCount, 1)
									log.Warn("h: %v update reply: %v, retry", h, reply)
								}
							}
						}
						rows = append(rows, createRow(h, subUserNames[i], updateMsg, updateMsg))
						keyArray = append(keyArray, subUserNames[i])
						loop++
						if i == len(subUserNames)-1 {
							log.Info("h: %v start %v end %v, size: %v", h, start, end, len(keyArray))
							for {
								reply := api.Insert(cfg.BenchConfig.DB, cfg.BenchConfig.Table, TableFields, rows)
								if reply.Code == 0 {
									atomic.AddInt64(&stat.lastUpdCount, 1)
									checkElapsedTimeBatch(h, keyArray, updateMsg)
									rows = make([][]interface{}, 0)
									keyArray = make([]string, 0)
									loop = 0
									break
								} else {
									atomic.AddInt64(&stat.errUpdCount, 1)
									log.Warn("h: %v update reply: %v, retry", h, reply)
								}
							}
						}
					} else {
						reply := api.Insert(cfg.BenchConfig.DB, cfg.BenchConfig.Table, TableFields,
							createRows(h, subUserNames[i], updateMsg, updateMsg))
						if reply.Code == 0 {
							atomic.AddInt64(&stat.lastUpdCount, 1)
							checkElapsedTime(h, subUserNames[i], updateMsg)
						} else {
							atomic.AddInt64(&stat.errUpdCount, 1)
							i = i - 1
							log.Warn("h: %v update reply: %v, retry", h, reply)
						}
					}
				}
			}()
		}
	}
}

// check correct when select after delete, update
func correctCheck4DelAndInsert() {
	if cfg.BenchConfig.Scope < 1 || cfg.BenchConfig.Scope > 16384 {
		log.Fatal("bench config scope should be between 1 and 16384")
	}
	insertMsg := fmt.Sprintf("insert message, %v", time.Now().Format("2006-01-02 15:04:05.000"))
	for i := 1; i <= cfg.BenchConfig.Scope; i++ {
		h := uint32(i - 1)
		userNames, err := selectSource(h)
		if err != nil || len(userNames) == 0 {
			log.Fatal("h %v no user", h)
		}

		log.Info("h:%v source data length: %v", h, len(userNames))

		pks := make(map[string]interface{})
		pks["h"] = h
		reply := api.Delete(cfg.BenchConfig.DB, cfg.BenchConfig.Table, HField, pks)
		if reply.Code != 0 {
			log.Fatal("h: %v delete failed, %v", h, reply.Message)
		}

		t1 := time.Now()
		for {
			leavedData, err := selectSource(h)
			if err != nil {
				log.Fatal("h: %v select leave data after delete error, %v", h, err)
			}
			if len(leavedData) == 0 {
				break
			}
		}
		log.Info("h: %v, delete success, elapsed time: %v", h, time.Since(t1))
		atomic.AddInt64(&stat.delCount, int64(len(userNames)))

		threadNum := cfg.BenchConfig.Threads
		increase := len(userNames)/threadNum + 1
		for concur := 0; concur < threadNum; concur++ {
			start := concur * increase
			end := start + increase
			if start >= len(userNames) {
				break
			}
			if end > len(userNames) {
				end = len(userNames)
			}
			go func() {
				subUserNames := userNames[start:end]
				rows := make([][]interface{}, 0)
				keyArray := make([]string, 0)
				var loop int
				for i := 0; i < len(subUserNames); i++ {
					if cfg.BenchConfig.Batch > 1 {
						if loop == cfg.BenchConfig.Batch {
							for {
								reply := api.Insert(cfg.BenchConfig.DB, cfg.BenchConfig.Table, TableFields, rows)
								if reply.Code == 0 {
									atomic.AddInt64(&stat.lastUpdCount, 1)
									checkElapsedTimeBatch(h, keyArray, insertMsg)
									rows = make([][]interface{}, 0)
									keyArray = make([]string, 0)
									loop = 0
									break
								} else {
									atomic.AddInt64(&stat.errUpdCount, 1)
									log.Warn("h: %v insert reply: %v, retry", h, reply)
								}
							}
						}
						rows = append(rows, createRow(h, subUserNames[i], insertMsg, insertMsg))
						keyArray = append(keyArray, subUserNames[i])
						loop++
						if i == len(subUserNames)-1 {
							for {
								reply := api.Insert(cfg.BenchConfig.DB, cfg.BenchConfig.Table, TableFields, rows)
								if reply.Code == 0 {
									atomic.AddInt64(&stat.lastUpdCount, 1)
									checkElapsedTimeBatch(h, keyArray, insertMsg)
									rows = make([][]interface{}, 0)
									keyArray = make([]string, 0)
									loop = 0
									break
								} else {
									atomic.AddInt64(&stat.errUpdCount, 1)
									log.Warn("h: %v insert reply: %v, retry", h, reply)
								}
							}
						}
					} else {
						reply := api.Insert(cfg.BenchConfig.DB, cfg.BenchConfig.Table, TableFields,
							createRows(h, subUserNames[i], insertMsg, insertMsg))
						if reply.Code == 0 {
							atomic.AddInt64(&stat.lastUpdCount, 1)
							checkElapsedTime(h, subUserNames[i], insertMsg)
						} else {
							atomic.AddInt64(&stat.errUpdCount, 1)
							i = i - 1
							log.Warn("h: %v insert reply: %v, retry", h, reply)
						}
					}
				}
			}()
		}
	}
}

//delete most data, and check the db data at rocksdb level
func correct4BatchDelete() {
	if cfg.BenchConfig.Scope < 1 || cfg.BenchConfig.Scope > 16384 {
		log.Fatal("bench config scope should be between 1 and 16384")
	}
	temp := uint32(0)
	concurrentC := &temp
	var pkMap map[uint32][]string
	pkMap = make(map[uint32][]string, cfg.BenchConfig.Scope)

	var lock sync.Mutex

	threadNum := cfg.BenchConfig.Threads
	scope := cfg.BenchConfig.Scope // 1 ~ 16384
	increase := scope/threadNum + 1
	for concur := 0; concur < threadNum; concur++ {
		start := concur * increase
		end := start + increase
		if start >= scope {
			break
		}
		if end > scope {
			end = scope
		}
		log.Info("handle h scope: start: %v, end: %v", start, end)
		go func(cc *uint32) {
			for i := start; i < end; i++ {
				h := uint32(i)
				userNames, err := selectSource(h)
				if err != nil {
					log.Error("h: %v select user error %v", h, err)
					atomic.AddUint32(cc, 1)
					continue
				}
				if len(userNames) == 0 {
					atomic.AddUint32(cc, 1)
					log.Warn("h: %v  no user data", h)
					continue
				}
				lock.Lock()
				pkMap[h] = userNames
				lock.Unlock()
				log.Info("h: %v source data length: %v", h, len(userNames))

				time.Sleep(time.Second)
				go func(cc2 *uint32) {
					defer atomic.AddUint32(cc2, 1)
					deleteByH(h, len(userNames))
				}(cc)
			}
		}(concurrentC)
	}

	log.Info("start to wait concurrent finish")

	for {
		log.Info("loop ============, %v", atomic.LoadUint32(concurrentC))
		if int(atomic.LoadUint32(concurrentC)) == cfg.BenchConfig.Scope {
			break
		}
		time.Sleep(time.Second)
	}

	log.Info("concurrent %v", atomic.LoadUint32(concurrentC))

	time.Sleep(10 * time.Minute)

	keyMap := make(map[string]uint8, 0)
	value := uint8(0)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 5; i++ {
		tempH := uint32(r.Intn(cfg.BenchConfig.Scope))
		userNames, ok := pkMap[tempH]
		if !ok || len(userNames) == 0 {
			i--
			continue
		}
		for _, u := range userNames {
			key, err := getKey(tempH, u)
			if err != nil {
				continue
			}
			log.Debug("pk: h:%d, user:%s, key:%v", tempH, u, string(key))
			keyMap[string(key)] = value
		}
	}

	currentTime := time.Now()
	for {
		if err := blob_store.CheckKey(path, keyMap); err != nil {
			log.Error("check key after delete error: %v", err)
		} else {
			break
		}
		time.Sleep(2 * time.Minute)
	}
	log.Info("delete from blob_dir elapsed time %v", time.Since(currentTime))
}

func getKey(h uint32, userName string) ([]byte, error) {
	type Match struct {
		column    string
		sqlValue  []byte
		matchType int
	}
	bytes := []byte(fmt.Sprintf("%v", h))
	var prefix []byte
	var err error
	if prefix, err = util.EncodePrimaryKey(prefix,
		&metapb.Column{Name: "h", Id: 1, DataType: metapb.DataType_BigInt, PrimaryKey: 1, Index: true},
		bytes); err != nil {
		return nil, err
	}

	if prefix, err = util.EncodePrimaryKey(prefix,
		&metapb.Column{Name: "user_name", Id: 2, DataType: metapb.DataType_Varchar, PrimaryKey: 1, Index: true},
		[]byte(userName)); err != nil {
		return nil, err
	}
	prefix = append(util.EncodeStorePrefix(util.Store_Prefix_KV, tableId), prefix...)
	return prefix, nil
}

func deleteByH(h uint32, sourceLength int) {
	pks := make(map[string]interface{})
	pks["h"] = h
	reply := api.Delete(cfg.BenchConfig.DB, cfg.BenchConfig.Table, HField, pks)
	if reply.Code != 0 {
		log.Fatal("h: %v delete failed, %v", h, reply)
	}

	t1 := time.Now()
	for {
		leavedData, err := selectSource(h)
		if err != nil {
			log.Warn("h: %v  select leave data after delete error, %v", h, reply)
			continue
		}
		if len(leavedData) == 0 {
			break
		}
	}
	log.Info("h: %v delete success, elapsed time: %v", h, time.Since(t1))
	atomic.AddInt64(&stat.delCount, int64(sourceLength))
}

func selectSource(h uint32) ([]string, error) {
	pks := make(map[string]interface{})
	pks["h"] = h
	var userNames []string

	for i := 0; ; i++ {
		limit_ := &server.Limit_{
			Offset:   uint64(i * 10000),
			RowCount: 10000,
		}
		reply := api.Select(cfg.BenchConfig.DB, cfg.BenchConfig.Table, UField, pks, limit_)
		if reply.Code != 0 {
			return nil, errors.New(fmt.Sprintf("select h %v source data: %s", h, reply.Message))
		}
		if len(reply.Values) == 0 {
			break
		}
		for _, row := range reply.Values {
			for _, r := range row {
				userName := r.(string)
				userNames = append(userNames, userName)
			}
		}
	}
	return userNames, nil
}

func checkElapsedTimeBatch(h uint32, userNames []string, firstUpdate string) {
	var pksMult []map[string]interface{}
	for _, userName := range userNames {
		pks := make(map[string]interface{})
		pks["h"] = h
		pks["user_name"] = userName
		pksMult = append(pksMult, pks)
	}

	currentTime := time.Now()
	flag := 0
	loop := 0
	for {
		loop++
		selectReply := api.MultSelect(cfg.BenchConfig.DB, cfg.BenchConfig.Table, PField, pksMult, nil)
		if selectReply.Code == 0 && len(selectReply.Values) > 0 {
			if len(selectReply.Values) != len(userNames) {
				flag = len(selectReply.Values)
			} else {
				for _, row := range selectReply.Values {
					for _, r := range row {
						passWord := r.(string)
						if passWord != firstUpdate {
							flag++
						}
					}
				}
			}
			break
		} else {
			log.Warn("retry select after update, h: %v, userName: [%v], reply: %v", h, userNames, selectReply)
		}
	}

	if flag > 0 {
		log.Error("multSelect after multUpdate:  update size %v, select size %v, loop: %v", len(userNames), flag, loop)
	} else {
		log.Warn("multSelect after multUpdate:  update size %v correct , elapsed time: %v, loop: %v", len(userNames), time.Since(currentTime), loop)
	}
}

func checkElapsedTime(h uint32, userName, firstUpdate string) {
	pks := make(map[string]interface{})
	pks["h"] = h
	pks["user_name"] = userName

	currentTime := time.Now()
	flag := 0
	loop := 0
	for {
		loop++

		selectReply := api.Select(cfg.BenchConfig.DB, cfg.BenchConfig.Table, PField, pks, nil)
		if selectReply.Code == 0 && len(selectReply.Values) > 0 {
			var passWords []string
			for _, row := range selectReply.Values {
				for _, r := range row {
					passWord := r.(string)
					passWords = append(passWords, passWord)
				}
			}
			if len(passWords) > 1 {
				flag = len(passWords)
				break
			}

			if passWords[0] != firstUpdate {
				continue
			}
			break
		}
	}

	if flag > 0 {
		log.Error("select after update:  %v values size: %v, should be 1, loop: %v", userName, flag, loop)
	} else {
		log.Warn("select after update::   %v update correct , elapsed time: %v, loop: %v", userName, time.Since(currentTime), loop)
	}
}

func createRows(h uint32, userName, passWord, realName string) [][]interface{} {
	rows := make([][]interface{}, 0)
	row := createRow(h, userName, passWord, realName)
	rows = append(rows, row)
	return rows
}

func createRowsNoPk(userName, passWord, realName string) [][]interface{} {
	rows := make([][]interface{}, 0)
	row := createRowNoPk(userName, passWord, realName)
	rows = append(rows, row)
	return rows
}

func createRow(h uint32, userName, passWord, realName string) []interface{} {
	row := make([]interface{}, 0)
	row = append(row, h)
	row = append(row, userName)
	row = append(row, passWord)
	row = append(row, realName)
	return row
}

func createRowNoPk(userName, passWord, realName string) []interface{} {
	row := make([]interface{}, 0)
	row = append(row, userName)
	row = append(row, passWord)
	row = append(row, realName)
	return row
}

func getUserName(no int, ip string, threadNo int) string {
	return fmt.Sprintf("%d-%s-%d", no, ip, threadNo)
}

func insertOrder(threadNo, total int, ip string) {
	real_name := makeString(cfg.BenchConfig.DataLen)
	pass_word := "pw"
	for no := 0; no < cfg.BenchConfig.SendNum; no++ {
		user_name := getUserName(no, ip, threadNo)
		h := hash(user_name) % 16384
		rows := createRows(h, user_name, pass_word, real_name)
		reply := api.Insert(cfg.BenchConfig.DB, cfg.BenchConfig.Table, TableFields, rows)
		log.Debug("%v", reply)
		if reply.Code == 0 {
			atomic.AddInt64(&stat.lastCount, 1)
		} else {
			atomic.AddInt64(&stat.errCount, 1)
			log.Warn("%v", reply)
		}
	}
}

func selectOrder(threadNo, total int, ip string) {
	pks := make(map[string]interface{})
	for no := 0; no < cfg.BenchConfig.SendNum; no++ {
		user_name := getUserName(no, ip, threadNo)
		h := hash(user_name) % 16384
		pks["user_name"] = user_name
		pks["h"] = h
		reply := api.Select(cfg.BenchConfig.DB, cfg.BenchConfig.Table, TableFields, pks, nil)
		log.Debug("userName %s, h %v, select result:%v", user_name, h, reply)
		if reply.Code == 0 && len(reply.Values) > 0 {
			atomic.AddInt64(&stat.lastSelCount, 1)
		} else {
			atomic.AddInt64(&stat.errSelCount, 1)
			if reply.Code == 0 {
				log.Debug("%v", reply)
			} else {
				log.Warn("execute failed, %v", reply)
			}
		}
	}
}

func rawSet(threadNo int, ip string) {
	for i := 0; i < cfg.BenchConfig.SendNum; i++ {
		key := fmt.Sprintf("%v_%v_%v", ip, threadNo, i) // ip_tid_no

		if err := api.RawSet(cfg.BenchConfig.DB, cfg.BenchConfig.Table, []byte(key), []byte(key)); err == nil {
			atomic.AddInt64(&stat.lastCount, 1)
		} else {
			log.Warn("raw set error: %v", err)

			atomic.AddInt64(&stat.errCount, 1)
			i = i - 1
		}
	}
}
func rawGet(threadNo int, ip string) {
	for i := 0; i < cfg.BenchConfig.SendNum; i++ {
		key := fmt.Sprintf("%v_%v_%v", ip, threadNo, i) // ip_tid_no

		_, err := api.RawGet(cfg.BenchConfig.DB, cfg.BenchConfig.Table, []byte(key))
		if err == nil {
			atomic.AddInt64(&stat.lastSelCount, 1)

			//if bytes.Compare([]byte(key), value) != 0 {
			//	log.Warn("raw get error: value not equal")
			//	atomic.AddInt64(&stat.errSelCount, 1)
			//}
		} else {
			log.Warn("raw get error: %v", err)
			atomic.AddInt64(&stat.errSelCount, 1)
		}
	}
}

func insertTestData(threadNo, total int, ip string) {
	real_name := randomString(cfg.BenchConfig.DataLen)
	pass_word := "pw"
	for i := 0; i < cfg.BenchConfig.SendNum; i++ {
		user_name := getUserName(i, ip, threadNo)
		h := hash(user_name) % 16384
		rows := createRows(h, user_name, pass_word, real_name)
		for b := 1; b < cfg.BenchConfig.Batch; b++ {
			row := createRow(h, fmt.Sprintf("%s-%d", user_name, b), pass_word, real_name)
			rows = append(rows, row)
		}
		reply := api.Insert(cfg.BenchConfig.DB, cfg.BenchConfig.Table, TableFields, rows)
		log.Debug("%v", reply)
		if reply.Code == 0 {
			atomic.AddInt64(&stat.lastCount, 1)
		} else {
			atomic.AddInt64(&stat.errCount, 1)
			i = i - 1
			log.Warn("%v", reply)
		}
	}

}

func insertNoPkTestData(threadNo, total int, ip string) {
	real_name := randomString(cfg.BenchConfig.DataLen)
	pass_word := "pw"
	TableFieldsNoPk := []string{"user_name", "pass_word", "real_name"}
	for i := 0; i < cfg.BenchConfig.SendNum; i++ {
		user_name := getUserName(i, ip, threadNo)
		rows := createRowsNoPk(user_name, pass_word, real_name)
		for b := 1; b < cfg.BenchConfig.Batch; b++ {
			row := createRowNoPk(fmt.Sprintf("%s-%d", user_name, b), pass_word, real_name)
			rows = append(rows, row)
		}
		reply := api.Insert(cfg.BenchConfig.DB, cfg.BenchConfig.Table, TableFieldsNoPk, rows)
		log.Debug("%v", reply)
		if reply.Code == 0 {
			atomic.AddInt64(&stat.lastCount, 1)
		} else {
			atomic.AddInt64(&stat.errCount, 1)
			//i = i - 1
			log.Warn("error reply:%v", reply)
		}
	}
}
