package elastic_search

import (
	"github.com/olivere/elastic"
	"time"
	"context"
	"fmt"
)

type Client struct {
	cli *elastic.Client
}

func NewElasticSearchClient(addresses []string) (*Client, error) {
	cli, err := elastic.NewClient(elastic.SetURL(addresses...), elastic.SetSniff(false))
	if err != nil {
		return nil, fmt.Errorf("elastic new client error: %v", err)
	}
	return &Client{
		cli: cli,
	}, nil
}

func (client *Client) Set(index, typ, id string, items interface{}, timeout time.Duration) (*elastic.IndexResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.cli.Index().
		Index(index).
		Type(typ).
		Id(id).
		BodyJson(items).
		Do(ctx)
}

func (client *Client) GetById(index, typ, id string, timeout time.Duration) (*elastic.GetResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.cli.Get().
		Index(index).
		Type(typ).
		Id(id).
		Do(ctx)
}

func (client *Client) Scan(index, term, value, sortBy string, limitFrom, limitTo int, timeout time.Duration) (*elastic.SearchResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	termQuery := elastic.NewTermQuery(term, value)
	return client.cli.Search().
		Index(index).
		Query(termQuery).
		//Sort(sortBy, true).
		//From(limitFrom).Size(limitTo).
		//Pretty(true).
		Do(ctx)
}

func (cli *Client) Close() {

}
