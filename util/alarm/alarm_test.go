package alarm

import (
	"testing"
	"context"
	"net/http"
	"time"
	"strconv"
	"model/pkg/alarmpb"
	"fmt"
	"net/http/httptest"
	"strings"
	"github.com/gomodule/redigo/redis"
)

func TestAlarmGrpc(t *testing.T) {
	//ctx, cancel:= context.WithCancel(context.Background())
	ctx, _ := context.WithCancel(context.Background())
	_, err := NewAlarmServer(ctx, 2222, "http://localhost:3333", "", "")
	if err != nil {
		t.Fatalf("NewAlarmServer failed: %v", err)
	}
	go func() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		l := r.Header.Get("len")
		t.Log("body len: ", l)
		l_, _ := strconv.ParseInt(l, 10, 64)
		buf := make([]byte, l_)
		n, err := r.Body.Read(buf)
		if n != int(l_) {
			t.Errorf("n: %v, err: %v, buf: %v", n, err, string(buf))
			return
		}
		defer r.Body.Close()
		t.Logf("http body: %v", string(buf[:n]))
		w.Write([]byte("ok"))
	})
	http.ListenAndServe(":3333", nil)
	}()

	time.Sleep(time.Second)
	cli, err := NewAlarmClient(":2222")
	if err != nil {
		t.Fatalf("NewAlarmClient failed: %v", err)
	}

	if err := cli.RangeNoHeartbeatAlarm(2, nil, "this range no hb", nil); err != nil {
		t.Fatalf("alarm range no hb error: %v", err)
	}

	time.Sleep(3*time.Second)
}

func TestAlarmMessageNotify(t *testing.T) {
	ctx := context.Background()
	s := newServer(ctx, "", "", "")

	appName := "gateway"
	clusterId := 10
	appAddr := "127.0.0.1"

	var samples []*Sample
	info := make(map[string]interface{})
	info["spaceId"] = clusterId
	info["ip"] = appAddr
	info["app_is_not_alive"] = 1
	info["app_name"] = appName
	samples = append(samples, NewSample("", 0, 0, info))
	fmt.Println("len samples: ", len(samples))
	req := &alarmpb.SimpleRequest{
		Head: &alarmpb.RequestHeader{ClusterId: int64(10)},
		Title: "title simple alarm",
		Content: "content simple alarm",
		SampleJson: SamplesToJson(samples),
	}
	s.SimpleAlarm(ctx, req)
	time.Sleep(3*time.Second)
}

func TestAlarmHandleAppPing(t *testing.T) {
	ctx := context.Background()
	s := newServer(ctx, "", "/redis/cluster/1:1803528818953446384", "192.168.150.61:5360") // do not send alarm really

	clusterId := 10
	appName := "gateway"
	ip0 := "192.168.0.0"
	ip1 := "192.168.0.1"
	ips := []string{ip0, ip1}
	ping_interval := 3
	url := fmt.Sprintf(`http://%s?cluster_id=%s&app_name=%s&ip_addrs=%s&ping_interval=%d`,
		"", clusterId, appName, strings.Join(ips, ","), ping_interval)

	var r *http.Request
	w := httptest.NewRecorder()

	// setex to jimdb
	r = httptest.NewRequest("GET", url, nil)
	s.HandleAppPing(w, r)

	// check jimdb
	appKey := s.genAliveAppKey(appName, fmt.Sprint(clusterId), ip0)
	t.Logf("test app key: %v", appKey)

	waitTicker := time.NewTicker(10*time.Second)
	for {
		select {
		case <-waitTicker.C:
			return
		default:
		reply, err := s.jimSendCommand("exists", appKey)
		if err != nil {
			t.Logf("jim send command error: %v", err)
		}
		replyInt, err := redis.Int(reply, err)
		if err != nil {
			t.Logf("jim command setex reply type is not int: %v", err)
		}

		if replyInt != 0 { // app key exists
			t.Logf("reply 1")
		} else {
			t.Fatal("reply 0")
		}
	}
	}
}

//func TestAlarmLoadAliveAppFromTableFbaseCluster(t *testing.T) {
//
//}
