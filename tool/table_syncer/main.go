package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	gw "proxy/gateway-server/server"
)

var confFile = flag.String("config", "./config.toml", "config file")

var cfg *Config

var kvURL string
var infoURL string
var fields []string

func getTableFields() {
	resp, err := http.Get(fmt.Sprintf("http://%s/tableinfo?dbname=%s&tablename=%s",
		cfg.Src.Gateway, cfg.Src.DB, cfg.Src.Table))
	if err != nil {
		panic(err)
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
		panic(err)
	}

	for _, c := range reply.Data.Columns {
		fields = append(fields, c.Column)
	}
	return
}

func fetchRows(client *http.Client, h int, offset uint64) (rows [][]interface{}, err error) {
	query := &gw.Query{
		DatabaseName: cfg.Src.DB,
		TableName:    cfg.Src.Table,
		Command: &gw.Command{
			Type:  "get",
			Field: fields,
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
			Field:  fields,
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

	fmt.Println("start to process h:", h)

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	var offset uint64
	var retries int
	var err error
	for {
		if retries > 10 {
			panic(err)
		}

		var rows [][]interface{}
		rows, err = fetchRows(client, h, offset)
		fmt.Println(rows, err)
		if err != nil {
			fmt.Printf("fetch rows[h:%d, offset:%d] failed: %v\n", h, offset, err)
			retries++
			continue
		}
		if len(rows) == 0 {
			break
		}

		// affected, err = putRows(client, rows)
		// fmt.Println(affected, err)
		offset += cfg.BatchSize
	}
}

func main() {
	flag.Parse()
}
