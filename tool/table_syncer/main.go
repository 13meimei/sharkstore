package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	gw "proxy/gateway-server/server"

	"github.com/BurntSushi/toml"
)

const (
	maxRetryTimes = 10
)

var confFile = flag.String("config", "./config.toml", "config file")

var cfg Config
var tableFields []string
var totalCount uint64

func getTableFields() ([]string, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/tableinfo?dbname=%s&tablename=%s",
		cfg.Src.Gateway, cfg.Src.DB, cfg.Src.Table))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var reply struct {
		Code int `json:"code"`
		Data struct {
			Columns []struct {
				Column string `json:"column_name"`
			} `json:"columns"`
		} `json:"data"`
		Msg string `json:"message"`
	}
	err = json.NewDecoder(resp.Body).Decode(&reply)
	if err != nil {
		return nil, err
	}

	fields := make([]string, 0, len(reply.Data.Columns))
	for _, c := range reply.Data.Columns {
		fields = append(fields, c.Column)
	}
	return fields, nil
}

func fetchRows(client *http.Client, h int, offset uint64) (rows [][]interface{}, err error) {
	query := &gw.Query{
		DatabaseName: cfg.Src.DB,
		TableName:    cfg.Src.Table,
		Command: &gw.Command{
			Type:  "get",
			Field: tableFields,
			Filter: &gw.Filter_{
				And: []*gw.And{&gw.And{
					Field: &gw.Field_{
						Column: "h",
						Value:  h,
					},
					Relate: "=",
				}},
				Limit: &gw.Limit_{
					Offset:   offset,
					RowCount: cfg.BatchSize,
				},
			},
		},
	}
	data, err := json.Marshal(query)
	if err != nil {
		panic(err)
	}

	url := "http://" + cfg.Src.Gateway + "/kvcommand"
	req, _ := http.NewRequest("POST", url, bytes.NewReader(data))
	req.Header.Set("Content-type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	var reply gw.Reply
	err = json.NewDecoder(resp.Body).Decode(&reply)
	if err != nil {
		panic(err)
	}
	if reply.Code != 0 {
		return nil, fmt.Errorf("code=%d", reply.Code)
	}
	return reply.Values, nil
}

func putRows(client *http.Client, rows [][]interface{}) (affected uint64, err error) {
	query := &gw.Query{
		DatabaseName: cfg.Dest.DB,
		TableName:    cfg.Dest.Table,
		Command: &gw.Command{
			Type:   "set",
			Field:  tableFields,
			Values: rows,
		},
	}
	data, err := json.Marshal(query)
	if err != nil {
		panic(err)
	}

	url := "http://" + cfg.Dest.Gateway + "/kvcommand"
	req, _ := http.NewRequest("POST", url, bytes.NewReader(data))
	req.Header.Set("Content-type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()

	var reply gw.Reply
	err = json.NewDecoder(resp.Body).Decode(&reply)
	if err != nil {
		panic(err)
	}
	if reply.Code != 0 {
		return 0, fmt.Errorf("code=%d", reply.Code)
	}
	return reply.RowsAffected, nil
}

func run(h int, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Println("start to process h:", h)

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	var offset uint64
	var retries int
	var err error
	for {
		if retries > maxRetryTimes {
			log.Panicf("sync hash %d offset %d failed: %v", h, offset, err)
		}

		var rows [][]interface{}
		rows, err = fetchRows(client, h, offset)
		log.Println(rows, err)
		if err != nil {
			log.Printf("fetch rows[h:%d, offset:%d] failed: %v\n", h, offset, err)
			retries++
			continue
		}
		if len(rows) == 0 {
			break
		}

		affected, err := putRows(client, rows)
		if err != nil || affected < uint64(len(rows)) {
			log.Printf("puts rows[h:%d, offset:%d] [rows:%d, affected: %d] failed: %v\n",
				h, offset, len(rows), affected, err)
			retries++
			continue
		}

		retries = 0
		atomic.AddUint64(&totalCount, uint64(len(rows)))
		offset += cfg.BatchSize
	}
}

func main() {
	flag.Parse()
	_, err := toml.DecodeFile(*confFile, &cfg)
	if err != nil {
		log.Panicf("load config file failed, err %v", err)
	}
	if err = cfg.Validate(); err != nil {
		log.Panicf("invalid config: %v", err)
	}
	log.Printf("config: %s", cfg.String())

	tableFields, err = getTableFields()
	if err != nil {
		log.Panicf("get table fields failed: %v", err)
	}
	log.Printf("table fields: %v", tableFields)

	hash := cfg.StartHash
	for hash < cfg.EndHash {
		concurrency := cfg.Concurrency
		if cfg.EndHash-hash < concurrency {
			concurrency = cfg.EndHash - hash
		}

		var wg sync.WaitGroup
		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			go run(hash, &wg)
			hash++
		}
		wg.Wait()
	}

	log.Printf("total synced: %d", atomic.LoadUint64(&totalCount))
}
