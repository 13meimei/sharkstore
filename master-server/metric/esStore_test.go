package metric

import (
	"testing"
	"time"
	"strings"
)

func TestClusterMetaMetricToEsStore(t *testing.T) {
	urls  := []string{"http://192.168.182.11:20001"}
	store := NewEsStore(urls, 2)
	err := store.Open()
	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	for i := 10; i < 20; i++ {
	meta := make(map[string]interface{})
	meta["cluster_id"] = i
	meta["total_capacity"] = 10
	meta["used_capacity"] = 1
	meta["range_count"] = 2
	meta["db_count"] = 2
	meta["table_count"] = 2
	meta["ds_count"] = 2
	meta["gs_count"] = 3
	fault_List := []string{"192.12.12.12","121.121.121.1"}
	meta["fault_list"] = strings.Join(fault_List, ";")
	meta["update_time"] = 1516121212

	err = store.Put(&Message{
		ClusterId: clusterId,
		Namespace: NAMESPACE_CLUSTER,
		Subsystem: METRIC_CLUSTER_META,
		Items: meta,
	})

	if err != nil {
		t.Errorf("test failed, err %v", err)
		time.Sleep(time.Second)
		return
	}
	}

	time.Sleep(20*time.Second)

}



