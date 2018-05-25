// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package boomer provides commands to run load tests and display results.
package boomer

import (
	"os"
	"os/signal"
	"sync"
	"time"
	"errors"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	"math/rand"
	"sync/atomic"
	"hash/fnv"
	"golang.org/x/net/context"
	"net/http"
	"net/url"
	"master-server/http_reply"
	"model/pkg/metapb"
	"reflect"
	"encoding/json"
)

type result struct {
	err           error
	statusCode    int
	duration      time.Duration
	contentLength int64
}

type Boomer struct {
	RangeNumber uint64
	MultiNumber uint64
	// N is the total number of requests to make.
	N int

	// C is the concurrency level, the number of concurrent workers to run.
	C int

	//
	User     string
	Password string
	Host     string
	Database string
	Table    string
	Protocol string

	// Qps is the rate limit.
	Qps int

	DLen int

	// Output represents the output type. If "csv" is provided, the
	// output will be dumped as a csv stream.
	Output string

	Times  int

	// 1: random insert
	// 2: order insert
	// 3: random select
	// 4: single select
	Mode   int

	// only for order insert and order select
	Base   int

	method string

	results chan *result

	lastCount int64

	preCount int64
	//records chan *record // verify area
	Verify bool
	StepVerify bool
	DeletePeerVerify uint64
	ClusterId uint64
	ClusterToken string
	MasterAddr string
}

func hash(s string) uint32 {
        h := fnv.New32a()
        h.Write([]byte(s))
        return h.Sum32()
}

func (b *Boomer) run(base int) {
	err := b.prepare(base)
	if err != nil {
		return
	}
	start := time.Now()
	b.runWorkers(base)
	newReport(b.method, b.results, b.Output, time.Now().Sub(start)).finalize()
}

// Run makes all the requests, prints the summary. It blocks until
// all work is done.
func (b *Boomer) Run() {
	switch b.Mode {
	case 1:
		b.method = "INSERT"
	case 2:
		b.method = "INSERT"
	case 3:
		b.method = "SELECT"
	case 4:
		b.method = "SELECT"
	case 5:
		b.method = "SELECT"
	case 6:
		b.method = "INSERT"
	case 7:
		b.method = "SELECT"
	case 8:
		b.method = "INSERT"
	}
	//b.records = make(chan *record, b.N)
	b.results = make(chan *result, b.N)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	start := time.Now()
	go func() {
		<-c
		// TODO(jbd): Progress bar should not be finalized.
		newReport(b.method, b.results, b.Output, time.Now().Sub(start)).finalize()
		fmt.Printf("\n\n")
		os.Exit(1)
	}()


	go func(){
		defer func(){
				fmt.Println("cal err ")
				if err := recover() ;err != nil {
					fmt.Println(err)
				}
			}()
		ticker := time.Tick(1 * time.Second)
	
		preTime := time.Now().UnixNano()
		for now := range ticker {
			total := atomic.LoadInt64(&b.lastCount)
			fmt.Printf("total:%d,ops:%d \n",total,(total - b.preCount) * 1000*1000*1000 / (now.UnixNano() - preTime))
			b.preCount = total
			preTime = now.UnixNano()
		}
	}()

	for i := 0; i < b.Times; i++ {
	    b.run(i)
	    fmt.Printf("\n\n")
	    time.Sleep(time.Second)
	}
	close(b.results)
}

func (b *Boomer) makeRandomInsert(db *sql.DB) {
	user := fmt.Sprintf("%s_%d", randomString(10), time.Now().UnixNano())
	password := fmt.Sprintf("%s_%d", randomString(10), time.Now().UnixNano())
	real := fmt.Sprintf("%s_%d", randomString(b.DLen), time.Now().UnixNano())
	SQL := fmt.Sprintf(`INSERT INTO %s(user_name, pass_word, real_name) VALUES("%s","%s","%s")`, b.Table, user, password, real)
	//id := fmt.Sprintf("%s_%d", randomString(8), time.Now().UnixNano())
	//name := fmt.Sprintf("%s_%d", randomString(8), time.Now().UnixNano())
	//SQL := fmt.Sprintf(`INSERT INTO %s(id, name) VALUES("%s","%s")`, b.Table, id, name)
	s := time.Now()
	var size int64
	var code int

	_, err := db.Exec(SQL)
	if err != nil {
		code = -1
	}
	atomic.AddInt64(&b.lastCount,1)
	b.results <- &result{
		statusCode:    code,
		duration:      time.Now().Sub(s),
		err:           err,
		contentLength: size,
	}
}

func (b *Boomer) makeHashMultiInsert(db *sql.DB,suffix string) error {
	curTime := time.Now().UnixNano()
	user := suffix
	h := hash(user)%(uint32(2+b.RangeNumber))
	sqlValues := make([]string, b.MultiNumber)
	for i := uint64(0); i < b.MultiNumber; i++ {
		keytp := fmt.Sprintf("%s_%d", randomString(10), curTime)
		host := fmt.Sprintf("%s_%d", randomString(b.DLen), curTime)
		timestamptp := time.Now().Unix()
		sqlValues[i] = fmt.Sprintf(`%d,"%s","%s",%d,%d,%d,%d,%d,%d,%d,%f,%d`, h, keytp, host, timestamptp, 0,0,0,0,0,0,1.0,0)
	}
	var sql string
	for i := uint64(0); i < b.MultiNumber; i++ {
		sql = sql+sqlValues[i]+","
	}
	sql = sql[:len(sql)-1]
	//fmt.Println("debug: sql value: ", sql)

	SQL := fmt.Sprintf(`INSERT INTO %s(h,keytp,host,timestamptp,tp90,tp99,tp999,avgtp,mintp,maxtp,succrate,calls) VALUES(%s)`, b.Table, sql)
	fmt.Println("SQL: ", SQL)
	_, err := db.Exec(SQL)
	if err != nil {
		//code = -1
	}
	atomic.AddInt64(&b.lastCount,1)
	return err
}

func (b *Boomer) makeHashInsert(db *sql.DB,suffix string) error {
	curTime := time.Now().UnixNano()
	user := suffix
	h := hash(user)%16384
	password := fmt.Sprintf("%s_%d", randomString(10), curTime)
	real := fmt.Sprintf("%s_%d", randomString(b.DLen), curTime)
	SQL := fmt.Sprintf(`INSERT INTO %s(h,user_name, pass_word, real_name) VALUES(%d,"%s","%s","%s")`, b.Table,h, user, password, real)
	//id := fmt.Sprintf("%s_%d", randomString(8), time.Now().UnixNano())
	//name := fmt.Sprintf("%s_%d", randomString(8), time.Now().UnixNano())
	//SQL := fmt.Sprintf(`INSERT INTO %s(id, name) VALUES("%s","%s")`, b.Table, id, name)
	//s := time.Now()
	//var size int64
	//var code int

	_, err := db.Exec(SQL)
	if err != nil {
		//code = -1
	}
	atomic.AddInt64(&b.lastCount,1)
	//b.results <- &result{
	//	statusCode:    code,
	//	duration:      time.Now().Sub(s),
	//	err:           err,
	//	contentLength: size,
	//}
	return err
}

func (b *Boomer) makeOrderInsert(db *sql.DB, suffix string) {
	user := fmt.Sprintf("user_%s", suffix)
	password := fmt.Sprintf("password_%s", suffix)
	real := fmt.Sprintf("real_%s_%s", randomString(b.DLen),suffix)
	SQL := fmt.Sprintf(`INSERT INTO %s(user_name, pass_word, real_name) VALUES("%s","%s","%s")`, b.Table, user, password, real)
	//id := fmt.Sprintf("%s_%d", randomString(8), time.Now().UnixNano())
	//name := fmt.Sprintf("%s_%d", randomString(8), time.Now().UnixNano())
	//SQL := fmt.Sprintf(`INSERT INTO %s(id, name) VALUES("%s","%s")`, b.Table, id, name)
	s := time.Now()
	var size int64
	var code int

	_, err := db.Exec(SQL)
	if err != nil {
		code = -1
	}
	atomic.AddInt64(&b.lastCount,1)
	b.results <- &result{
		statusCode:    code,
		duration:      time.Now().Sub(s),
		err:           err,
		contentLength: size,
	}
}

func (b *Boomer) makeOrderSelect(db *sql.DB, suffix string) {
	user := fmt.Sprintf("user_%s", suffix)
	SQL := fmt.Sprintf(`SELECT user_name, pass_word, real_name FROM %s WHERE user_name="%s"`, b.Table, user)
	s := time.Now()
	var size int64
	var code int

	var user_name, pass_word, real_name string
	err := db.QueryRow(SQL).Scan(&user_name, &pass_word, &real_name)
	if err != nil {
		code = -1
	}
	atomic.AddInt64(&b.lastCount,1)
	b.results <- &result{
		statusCode:    code,
		duration:      time.Now().Sub(s),
		err:           err,
		contentLength: size,
	}
}

func (b *Boomer) makeSignalSelect (db *sql.DB) {
	user := "user_1"
	SQL := fmt.Sprintf(`SELECT user_name, pass_word, real_name FROM %s WHERE user_name="%s"`, b.Table, user)

	s := time.Now()
	var size int64
	var code int

	var user_name, pass_word, real_name string
	err := db.QueryRow(SQL).Scan(&user_name, &pass_word, &real_name)
	if err != nil {
		code = -1
	}
	atomic.AddInt64(&b.lastCount,1)
	b.results <- &result{
		statusCode:    code,
		duration:      time.Now().Sub(s),
		err:           err,
		contentLength: size,
	}
}

func (b *Boomer) makeRandomSelect(db *sql.DB, suffix string) {
	user := fmt.Sprintf("user_%s", suffix)
	SQL := fmt.Sprintf(`SELECT user_name, pass_word, real_name FROM %s WHERE user_name="%s"`, b.Table, user)
	s := time.Now()
	var size int64
	var code int

	var user_name, pass_word, real_name string
	err := db.QueryRow(SQL).Scan(&user_name, &pass_word, &real_name)
	if err != nil {
		code = -1
	}
	atomic.AddInt64(&b.lastCount,1)
	b.results <- &result{
		statusCode:    code,
		duration:      time.Now().Sub(s),
		err:           err,
		contentLength: size,
	}
}

func (b *Boomer) makeHashSelect(db *sql.DB, suffix string) error {
	user := suffix
	h := hash(user)%16384
	SQL := fmt.Sprintf(`SELECT user_name, pass_word, real_name FROM %s WHERE h=%d and user_name="%s"`, b.Table,h, user)
	//s := time.Now()
	//var size int64
	//var code int

	var user_name, pass_word, real_name string
	err := db.QueryRow(SQL).Scan(&user_name, &pass_word, &real_name)
	if err != nil {
		//code = -1
	}else if len(user_name) <= 0 {
		//code = -1
		err = errors.New("user_name is empty")
	}
	atomic.AddInt64(&b.lastCount,1)
	//b.results <- &result{
	//	statusCode:    code,
	//	duration:      time.Now().Sub(s),
	//	err:           err,
	//	contentLength: size,
	//}
	return err
}

func (b *Boomer) verifyWrite(db *sql.DB,suffix string, i int) {
	for { // write until ok
		if err := b.makeHashInsert(db,suffix); err != nil {
			fmt.Printf("verify insert line %v suffix %v error: %v\n", i, suffix, err)
		} else {
			return
		}
	}
}

func (b *Boomer) verifyRead(db *sql.DB,suffix string, i int, logPoint string) {
	for { // write read ok
		if err := b.makeHashSelect(db,suffix); err != nil {
			fmt.Printf("verify select line %v suffix %v error(%v): %v\n", i, suffix, logPoint, err)
		} else {
			return
		}
	}
}

func (b *Boomer) getRouteCommand(cli *http.Client, args url.Values) *http_reply.RangeLocateResponse {
	url := fmt.Sprintf("http://%v/range/locate?%v", b.MasterAddr, args.Encode())
	reply, err := httpGetCommand(cli, url)
	if err != nil {
		fmt.Println("master command peer delete error: ", err)
		return nil
	}
	fmt.Println("get route reply: ", reply)
	if reply == nil {
		return nil
	}
	repStr, ok := reply.(string)
	if !ok {
		fmt.Println("reply type is: ", reflect.TypeOf(reply))
		return nil
	}
	rep := new(http_reply.RangeLocateResponse)
	if err := json.Unmarshal([]byte(repStr), rep); err != nil {
		fmt.Println("reply unmarshal error: ", err)
		return nil
	}
	return rep
}

func (b *Boomer) delPeerCommand(cli *http.Client, args url.Values) string {
	url := fmt.Sprintf("http://%v/peer/delete_force?%v", b.MasterAddr, args.Encode())
	reply, err := httpGetCommand(cli, url)
	if err != nil {
		return err.Error()
	}
	if reply != nil {
		// type assert, return
	}

	return ""
}

func httpGetCommand(cli *http.Client, cmd string) (interface{}, error) {
	resp, err := cli.Get(cmd)
	if err != nil {
		return nil, fmt.Errorf("http getcommand %v error: http get: %v", cmd, err)
	}
	defer resp.Body.Close()

	reply, err := http_reply.GetResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("http getcommand %v error: read response %v", cmd, err)
	}
	if reply.Code != 0 {
		return nil, fmt.Errorf("http getcommand %v error: read response %v", cmd, reply.Message)
	}
	return reply.Data, nil
}

func selectDownPeer(routes []*http_reply.Route) (uint64, uint64, bool) {
	if len(routes) == 0 {
		fmt.Println("select down peer route len is 0")
		return 0, 0, false
	}
	set := func() []*http_reply.Route {
		var ret_ []*http_reply.Route
		for _, r := range routes {
			if r.Leader == nil {
				continue
			}
			if len(r.Range.Peers) < 2 {
				continue
			}
			if r.Leader.Node.GetState() != metapb.NodeState_N_Login {
				continue
			}
			PEER_LOOP:
			for _, p := range r.Range.Peers {
				if p.Node.GetState() != metapb.NodeState_N_Login {
					break PEER_LOOP
				}
			}
			if len(r.Downs) != 0 || len(r.Pends) != 0 {
				continue
			}
			ret_ = append(ret_, r)
		}
		return ret_
	}()
	if len(set) == 0 {
		return 0, 0, false
	}

	rand.Seed(time.Now().Unix())
	route := set[rand.Intn(len(set))]

	return route.Range.Id, route.Leader.Id, true

	//n := rand.Intn(1+len(route.Range.Peers))
	//if n == len(route.Range.Peers) {
	//	return route.Range.Id, route.Leader.Id, true
	//} else {
	//	return route.Range.Id, route.Range.Peers[n].Id, true
	//}
}

func (b *Boomer) verifyWithPeerDown(ctx context.Context) {
	t := time.NewTimer(time.Second*time.Duration(b.DeletePeerVerify))
	cli := &http.Client{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// get route
			d := time.Now().Unix()
			s := http_reply.GenSign(b.ClusterId, d, b.ClusterToken)
			args := url.Values{}
			args.Add("dbName", fmt.Sprintf("%v", b.Database))
			args.Add("tableName", fmt.Sprintf("%v", b.Table))
			args.Add("d", fmt.Sprintf("%v", d))
			args.Add("s", fmt.Sprintf("%v", s))
			routes := b.getRouteCommand(cli, args)
			if routes == nil {
				fmt.Println("route is nil")
				t.Reset(time.Second*time.Duration(b.DeletePeerVerify))
				continue
			}
			fmt.Println("get route: ", routes.Routes)
			rangeId, peerId, ok := selectDownPeer(routes.Routes)
			if !ok {
				fmt.Println("node peer selected")
				t.Reset(time.Second*time.Duration(b.DeletePeerVerify))
				continue
			}

			fmt.Printf("rangeid: %v, peerid: %v\n", rangeId, peerId)
			// down peer to master
			args = url.Values{}
			args.Add("dbName", fmt.Sprintf("%v", b.Database))
			args.Add("tableName", fmt.Sprintf("%v", b.Table))
			args.Add("rangeId", fmt.Sprintf("%v", rangeId))
			args.Add("peerId", fmt.Sprintf("%v", peerId))
			args.Add("d", fmt.Sprintf("%v", d))
			args.Add("s", fmt.Sprintf("%v", s))
			if err := b.delPeerCommand(cli, args); len(err) != 0 {
				fmt.Println("delete peer error: %v", err)
			}

			t.Reset(time.Second*time.Duration(b.DeletePeerVerify))
		}
	}
}

// index: c, base: times, n: n/c
func (b *Boomer) runWorker(index, base, n int) {
	var throttle <-chan time.Time
	if b.Qps > 0 {
		throttle = time.Tick(time.Duration(1e6/(b.Qps)) * time.Microsecond)
	}

	dns := fmt.Sprintf("%s:%s@tcp(%s)/%s", b.User, b.Password, b.Host, b.Database)
	db, err := sql.Open("mysql", dns)
	if err != nil {
		fmt.Println(err)
		return
	}
	// run verify
	if b.Verify {
		fmt.Println("---- verify mode ----")
		fmt.Println("verify: ", b.Verify)
		fmt.Println("verify step: ", b.StepVerify)
		fmt.Println("id: ", b.ClusterId)
		fmt.Println("token: ", b.ClusterToken)
		fmt.Println("master: ", b.MasterAddr)
		for i := 0; i < n; i++ {
			suffix := fmt.Sprintf("%d_%d_%d_%d", b.Base + base, base, index,index*n + i)
			b.verifyWrite(db, suffix, i)
			if b.StepVerify { // read until ok
				//fmt.Printf("verify step line: %v\n", i)
				b.verifyRead(db, suffix, i, "step")
			}
		}
		for i := 0; i < n; i++ {
			//fmt.Printf("verify end line: %v\n", i)
			suffix := fmt.Sprintf("%d_%d_%d_%d", b.Base + base, base, index,index*n + i)
			b.verifyRead(db, suffix, i, "end")
		}
		fmt.Println("---- verify ok ----")
		return
	}

	// run mode
	for i := 0; i < n; i++ {
		if b.Qps > 0 {
			<-throttle
		}

		switch b.Mode {
		// 1: random insert
		case 1:
			b.makeRandomInsert(db)
		// 2: order insert
		case 2:
			suffix := fmt.Sprintf("%d_%d_%d_%d", b.Base + base, base, index,index*n + i)
			b.makeOrderInsert(db, suffix)
		// 3: order select
		case 3:
			suffix := fmt.Sprintf("%d_%d_%d_%d", b.Base + base, base, index, index*n + i)
			b.makeOrderSelect(db, suffix)
		// 4: single select
		case 4:
			b.makeSignalSelect(db)
		// 5: random select
		case 5:
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			var _base, _index, _i int
			_base = r.Intn(b.Times)
			_index = r.Intn(b.C)
			_i = r.Intn(n)
			suffix := fmt.Sprintf("%d_%d_%d_%d", b.Base + _base, _base, _index, _i)
			b.makeRandomSelect(db, suffix)
		case 6:
			suffix := fmt.Sprintf("%d_%d_%d_%d", b.Base + base, base, index,index*n + i)
			b.makeHashInsert(db,suffix)
		case 7:
			//r := rand.New(rand.NewSource(time.Now().UnixNano()))
			//var _base, _index, _i int
			//_base = r.Intn(b.Times)
			//_index = r.Intn(b.C)
			//_i := r.Intn(n)
			suffix := fmt.Sprintf("%d_%d_%d_%d", b.Base + base, base, index,index*n + i)
			b.makeHashSelect(db,suffix)
		case 8:
			suffix := fmt.Sprintf("%d_%d_%d_%d", b.Base + base, base, index,index*n + i)
			b.makeHashMultiInsert(db,suffix)
		}
	}
	db.Close()
}

func (b *Boomer) prepare(base int) error {
	dns := fmt.Sprintf("%s:%s@tcp(%s)/%s", b.User, b.Password, b.Host, b.Database)
	db, err := sql.Open("mysql", dns)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer db.Close()
	switch b.Mode {
	// 1: random insert
	case 1:
	// 2: order insert
	case 2:
	// 3: order select
	case 3:
		//var wg sync.WaitGroup
		//wg.Add(b.C)
		//for i := 0; i < b.C; i++ {
		//	go func(_index, _base, _n int) {
		//		defer wg.Done()
		//		dns := fmt.Sprintf("%s:%s@tcp(%s)/%s", b.User, b.Password, b.Host, b.Database)
		//		db, err := sql.Open("mysql", dns)
		//		if err != nil {
		//			fmt.Println(err)
		//			os.Exit(-1)
		//		}
		//		for j := 0; j < _n; j++ {
		//			suffix := fmt.Sprintf("%d_%d_%d", _base, _index, j)
		//			user := fmt.Sprintf("user_%s", suffix)
		//			password := fmt.Sprintf("password_%s", suffix)
		//			real := fmt.Sprintf("real_%s", suffix)
		//			SQL := fmt.Sprintf(`INSERT INTO %s(user_name, pass_word, real_name) VALUES("%s","%s","%s")`, b.Table, user, password, real)
		//			_, err := db.Exec(SQL)
		//			if err != nil {
		//				fmt.Println(err)
		//				os.Exit(-1)
		//			}
		//		}
		//	}(i, base, b.N / b.C)
		//}
		//wg.Wait()
	// 4: single select
	case 4:
		suffix := "1"
		user := fmt.Sprintf("user_%s", suffix)
		password := fmt.Sprintf("password_%s", suffix)
		real := fmt.Sprintf("real_%s", suffix)
		SQL := fmt.Sprintf(`SELECT user_name, pass_word, real_name FROM %s WHERE user_name="%s"`, b.Table, user)
		var user_name, pass_word, real_name string
		err := db.QueryRow(SQL).Scan(&user_name, &pass_word, &real_name)
		if err == nil {
			return nil
		}
		SQL = fmt.Sprintf(`INSERT INTO %s(user_name, pass_word, real_name) VALUES("%s","%s","%s")`, b.Table, user, password, real)
		_, err = db.Exec(SQL)
		if err != nil {
			return err
		}
	// 5 random select
	case 5:
	case 6:

	}
	return nil
}

func (b *Boomer) runWorkers(base int) {
	var wg sync.WaitGroup
	wg.Add(b.C)

	ctx, cancel := context.WithCancel(context.Background())
	if b.Verify && b.DeletePeerVerify != 0 {
		go b.verifyWithPeerDown(ctx)
	}

	// Ignore the case where b.N % b.C != 0.
	for i := 0; i < b.C; i++ {
		go func(index int) {
			b.runWorker(index, base, b.N / b.C)
			wg.Done()
		}(i)
		// TODO rm time.Sleep(time.Second*time.Duration(b.DeletePeerVerify)*10)
	}
	wg.Wait()
	cancel()
}

func randomString(length int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < length; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}

