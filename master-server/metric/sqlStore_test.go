package metric

import "testing"

func TestPrepare(t *testing.T) {
	net := make(map[string]interface{})
	net["cluster_id"] = 1
	net["tps"] = float32(1.2)
	net["create_time"] = float64(2.4)
	net["min_tp"] = "test"
	net["max_tp"] = uint(2)
	net["avg_tp"] = uint8(3)
	net["tp50"] = uint16(4)
	net["tp90"] = uint32(5)
	net["tp99"] = uint64(6)
	net["tp999"] = int(2)
	net["net_in_per_sec"] = int8(3)
	net["net_out_per_sec"] = int16(4)
	net["clients_count"] = int32(5)
	net["open_clients_per_sec"] = int64(6)

	format, args, err := prepare(&Message{
		ClusterId: 1,
		Namespace: "cluster",
		Subsystem: "cluster_net",
		Items: net,
	})
	if err != nil {
		t.Errorf("test error %v", err)
		return
	}
	t.Logf("%s, %v", format, args)
}
