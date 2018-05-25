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

// Package requester provides commands to run load tests and display results.
package requester

import (
	"sync"
	"time"

	"model/pkg/kvrpcpb"
	dsClient "pkg-go/ds_client"
	"golang.org/x/net/context"
	"model/pkg/metapb"
	"proxy/gateway-server/server"
	"proxy/store/dskv"
	"fmt"
)

type result struct {
	err           error
	statusCode    int
	duration      time.Duration
	contentLength int64
}

type Work struct {
	// N is the total number of requests to make.
	N int
	// C is the concurrency level, the number of concurrent workers to run.
	C int
	Host     string
	Protocol string
	// Qps is the rate limit.
	Qps int
	// Output represents the output type. If "csv" is provided, the
	// output will be dumped as a csv stream.
	Output string
	Times  int
	// 1: KvRawGet
	// 2: KvRawPut
	// 3: KvRawDelete
	// 4: KvRawExecute
	// 5: KvSelect
	// 6: KvInsert
	// 7: KvDelete
	// 8: CreateRange
	// 9: DeleteRange
	// 10: TransferLeader
	Mode int
	method string
	kvCli dsClient.KvClient
	schClient dsClient.SchClient
	results chan *result
	start   time.Time
	table	*server.Table
	in 	*dskv.Request
}

// Run makes all the requests, prints the summary. It blocks until
// all work is done.
func (b *Work) Run() {
	b.prepare()

	b.results = make(chan *result, b.N)
	b.start = time.Now()

	b.runWorkers()
	b.Finish()
	close(b.results)
}

func (b *Work) Finish() {
	total := time.Now().Sub(b.start)
	newReport(b.method, b.results, b.Output, total).finalize()
}

func getRows() []*kvrpcpb.KeyValue {
	var kvGroup []*kvrpcpb.KeyValue
	var kv *kvrpcpb.KeyValue
	for i:=0; i< 3; i++ {
		kv = &kvrpcpb.KeyValue{
			Key:     []byte("user_name"),
			Value:   []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"),
			ExpireAt: 1000000,
		}
		kvGroup = append(kvGroup, kv)
	}
	return kvGroup
}

func (b *Work) makeRequest() {
	switch b.Mode {
	case 0:
		b.makeSimpleBack()
	case 1:
		b.makeKvRawGet()
	case 2:
		b.makeKvRawPut()
	case 3:
		b.makeKvRawDelete()
	case 4:
		b.makeKvRawExecute()
	case 5:
		b.makeKvSelect()
	case 6:
		b.makeKvInsert()
	case 7:
		b.makeKvDelete()
	case 8:
		b.makeCreateRange()
	case 9:
		b.makeDeleteRange()
	case 10:
		b.makeTransferLeader()
	}
}

func (b *Work)  initSelectRequest(){
	fieldList := make([]*kvrpcpb.SelectField, 0, len(b.table.Columns))
	for _, mc := range b.table.GetColumns() {
		fieldList = append(fieldList, &kvrpcpb.SelectField{
			Typ:    kvrpcpb.SelectField_Column,
			Column: mc,
		})
	}
	pbMatches := make([]*kvrpcpb.Match, 0, 1)
	pbMatches = append(pbMatches, &kvrpcpb.Match{
		Column:    b.table.FindColumn("user_name"),
		Threshold: []byte("user_1_0_0_0"),
		MatchType: kvrpcpb.MatchType(5),
		})
	/**
		scope := &kvrpcpb.Scope{
			Start: []byte("\001\000\000\000\000\000\000\000\003\022user_1_0_0_0\000\002"),
			Limit: []byte("\001\000\000\000\000\000\000\000\004"),
		}

		req := &kvrpcpb.DsKvSelectRequest{
			Header: &kvrpcpb.RequestHeader{
				RangeId: uint64(4),
				RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
			},
			Req:  &kvrpcpb.KvSelectRequest{
				Scope: scope,
				FieldList:    fieldList,
				WhereFilters: pbMatches,
				//Limit:        subLimit,
			},
		}
		b.in = &dskv.Request{
			Type: kvrpcpb.Type_Select,
			SelectReq: req,
		}
		*/
}

func (b *Work)  initInsertRequest(){
	/**
	req := &kvrpcpb.DsKvInsertRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:  &kvrpcpb.KvInsertRequest{
			Rows:           getRows(),
		},
	}
	b.in = &dskv.Request{
		Type: kvrpcpb.Type_Insert,
		InsertReq: req,
	}
	*/
}

func (b *Work) prepare() {
	table := &metapb.Table{
		Name:   "fbase",
		DbName: "test",
		Id:     3,
		DbId:   2,
		Columns:    []*metapb.Column{
			{Name: "user_name", Id:uint64(1), DataType: metapb.DataType_Varchar, PrimaryKey:uint64(1), Index:true },
			{Name: "pass_word", Id:uint64(2), DataType: metapb.DataType_Varchar, Index:true},
			{Name: "real_name", Id:uint64(3), DataType: metapb.DataType_Varchar, Index:true},
		},
		Epoch:      &metapb.TableEpoch{ConfVer: uint64(1), Version: uint64(7)},
		CreateTime: time.Now().Unix(),
		PkDupCheck: false,
	}
	b.table = server.NewTable(table, nil, 5*time.Minute)
	switch b.Mode {
	case 0:
		b.method = "simpleBack"
		b.kvCli = dsClient.NewRPCClient(1)
	case 1:
		b.method = "KvRawGet"
		b.kvCli = dsClient.NewRPCClient(1)
	case 2:
		b.method = "KvRawPut"
		b.kvCli = dsClient.NewRPCClient(1)
	case 3:
		b.method = "KvRawDelete"
		b.kvCli = dsClient.NewRPCClient(1)
	case 4:
		b.method = "KvRawExecute"
		b.kvCli = dsClient.NewRPCClient(1)
	case 5:
		b.method = "KvSelect"
		b.kvCli = dsClient.NewRPCClient(1)
		b.initSelectRequest()
	case 6:
		b.method = "KvInsert"
		b.kvCli = dsClient.NewRPCClient(1)
		b.initInsertRequest()
	case 7:
		b.method = "KvDelete"
		b.kvCli = dsClient.NewRPCClient(1)
	case 8:
		b.method = "CreateRange"
		b.schClient = dsClient.NewSchRPCClient(1)
	case 9:
		b.method = "DeleteRange"
		b.schClient = dsClient.NewSchRPCClient(1)
	case 10:
		b.method = "TransferLeader"
		b.schClient = dsClient.NewSchRPCClient(1)
	}
}

func (b *Work) runWorker(n int) {
	var throttle <-chan time.Time
	if b.Qps > 0 {
		throttle = time.Tick(time.Duration(1e6/(b.Qps)) * time.Microsecond)
	}
	for i := 0; i < n; i++ {
		if b.Qps > 0 {
			<-throttle
		}
		b.makeRequest()
	}
}

func (b *Work) runWorkers() {
	var wg sync.WaitGroup
	wg.Add(b.C)

	// Ignore the case where b.N % b.C != 0.
	for i := 0; i < b.C; i++ {
		go func() {
			b.runWorker(b.N / b.C)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (b *Work) makeSimpleBack()  {
	/**
	req := &kvrpcpb.DsKvInsertRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:  &kvrpcpb.KvInsertRequest{
			Rows:           getRows(),
			CheckDuplicate: false,
			Timestamp:      nil,
		},
	}
	ctx, _ := context.WithTimeout(context.Background(), 20 * time.Second )
	s := time.Now()
	var size int64
	var code int
	_, err := b.kvCli.Insert(ctx, b.Host, req)
	if err != nil {
		code = -1
	}
	b.results <- &result{
		statusCode:    code,
		duration:      time.Now().Sub(s),
		err:           err,
		contentLength: size,
	}
	*/
}

func (b *Work) makeKvRawGet() {
	//req := &kvrpcpb.DsKvRawGetRequest{
	//	Header: &kvrpcpb.RequestHeader{},
	//	Req:  &kvrpcpb.KvRawGetRequest{
	//		Key: []byte("abcd"),
	//	},
	//}
	//ctx, _ := context.WithTimeout(context.Background(), 20 * time.Second )
	//s := time.Now()
	//var size int64
	//var code int
	//_, err := b.kvCli.RawGet(ctx, b.Host, req)
	//if err != nil {
	//	code = -1
	//}
	//b.results <- &result{
	//	statusCode:    code,
	//	duration:      time.Now().Sub(s),
	//	err:           err,
	//	contentLength: size,
	//}

}

func (b *Work) makeKvRawPut() {
	//req := &kvrpcpb.DsKvRawPutRequest{
	//	Header: &kvrpcpb.RequestHeader{},
	//	Req:  &kvrpcpb.KvRawPutRequest{
	//		Key:   []byte("abcd"),
	//		Value: []byte("abcd"),
	//	},
	//}
	//ctx, _ := context.WithTimeout(context.Background(), 20 * time.Second )
	//s := time.Now()
	//var size int64
	//var code int
	//_, err := b.kvCli.RawPut(ctx, b.Host, req)
	//if err != nil {
	//	code = -1
	//}
	//b.results <- &result{
	//	statusCode:    code,
	//	duration:      time.Now().Sub(s),
	//	err:           err,
	//	contentLength: size,
	//}

}
func (b *Work) makeKvRawDelete() {
	//req := &kvrpcpb.DsKvRawDeleteRequest{
	//	Header: &kvrpcpb.RequestHeader{},
	//	Req:  &kvrpcpb.KvRawDeleteRequest{
	//		Key:   []byte("abcd"),
	//	},
	//}
	//ctx, _ := context.WithTimeout(context.Background(), 20 * time.Second )
	//s := time.Now()
	//var size int64
	//var code int
	//_, err := b.kvCli.RawDelete(ctx, b.Host, req)
	//if err != nil {
	//	code = -1
	//}
	//b.results <- &result{
	//	statusCode:    code,
	//	duration:      time.Now().Sub(s),
	//	err:           err,
	//	contentLength: size,
	//}

}
func (b *Work) makeKvRawExecute() {
}
func (b *Work) makeKvSelect()  {
	ctx, _ := context.WithTimeout(context.Background(), 20 * time.Second )
	s := time.Now()
	var size int64
	var code int
	_resp, err := b.kvCli.Select(ctx, b.Host, b.in.GetSelectReq())
	if err != nil {
		code = -1
	}else {
		if _resp.GetResp().GetCode() != 0 {
			fmt.Printf(fmt.Sprintf("remote server return code: %v", _resp.GetResp().GetCode()))
		}else {
			fmt.Println("result : = " + _resp.GetResp().String())
		}
	}
	b.results <- &result{
		statusCode:    code,
		duration:      time.Now().Sub(s),
		err:           err,
		contentLength: size,
	}
}
func (b *Work) makeKvInsert() {
	ctx, _ := context.WithTimeout(context.Background(), 20 * time.Second )
	s := time.Now()
	var size int64
	var code int
	_, err := b.kvCli.Insert(ctx, b.Host, b.in.GetInsertReq())
	if err != nil {
		code = -1
	}
	b.results <- &result{
		statusCode:    code,
		duration:      time.Now().Sub(s),
		err:           err,
		contentLength: size,
	}
}
func (b *Work) makeKvDelete() {
	/**
	req := &kvrpcpb.DsDeleteRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req:  &kvrpcpb.DsDeleteRequest{
			Key: []byte("abcd"),
		},
	}
	ctx, _ := context.WithTimeout(context.Background(), 20 * time.Second )
	s := time.Now()
	var size int64
	var code int
	_, err := b.kvCli.Delete(ctx, b.Host, req)
	if err != nil {
		code = -1
	}
	b.results <- &result{
		statusCode:    code,
		duration:      time.Now().Sub(s),
		err:           err,
		contentLength: size,
	}
	*/

}
func (b *Work) makeCreateRange()  {
	r := &metapb.Range{
		Id:     4,
		StartKey: []byte("\001\000\000\000\000\000\000\000\003"),
		EndKey: []byte("\001\000\000\000\000\000\000\000\004"),
		RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
		Peers: []*metapb.Peer{&metapb.Peer{Id: 5, NodeId: 1}},
		TableId: 3,
	}
	s := time.Now()
	var size int64
	var code int
	err := b.schClient.CreateRange(b.Host, r)
	if err != nil {
		code = -1
	}
	b.results <- &result{
		statusCode:    code,
		duration:      time.Now().Sub(s),
		err:           err,
		contentLength: size,
	}

}
func (b *Work) makeDeleteRange()  {

}
func (b *Work) makeTransferLeader()  {

}
