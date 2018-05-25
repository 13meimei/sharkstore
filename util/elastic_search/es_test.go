package elastic_search

import (
	"testing"
	"time"
	"fmt"
)

func TestEs(t *testing.T) {
	cli, _ := NewElasticSearchClient([]string{"http://192.168.182.11:20001"})
	//getRes, err := cli.GetById("task_stats", "task_stats", "", 3*time.Second)
	//if err != nil {
	//	t.Fatal("get error: ", err)
	//}
	//t.Logf("get reply: %+v", getRes)
	items := make(map[string]interface{})
	items["id"] = 123123
	items["result"] = "321321"
	setRes, err := cli.Set("task_stats", "task_stats", "123123", items, time.Second)
	if err != nil {
		t.Fatal("insert error: ", err)
	}
	fmt.Printf("set reply: %+v\n", setRes)

	scanRes, err := cli.Scan("task_stats", "result", "testing", "result", 0, 2, time.Second)
	if err != nil {
		t.Fatal("scan error: ", err)
	}
	fmt.Printf("get reply: %+v\n", scanRes)
	fmt.Printf("hits: %+v\n", *scanRes.Hits)
	for i := int64(0); i < scanRes.TotalHits(); i++ {
		fmt.Printf("hit[%v]: %+v\n", i, string(*scanRes.Hits.Hits[i].Source))

	}
}
