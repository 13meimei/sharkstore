package alarm2

import (
	"testing"
	"model/pkg/alarmpb2"
	"github.com/muesli/cache2go"
	"time"
	"errors"
	"fmt"
)

var conf = Alarm2ServerConfig{
	ServerPort: 9999,
	MysqlArgs: "",
	MysqlPullingDurationSec: 2,
	AppAliveCheckingDurationSec: 2,
	AlarmGatewayAddr: "",
	JimUrl: "",
	JimApAddr: "",
}

var (
	gAppClusterId 		= int64(12)
	gAppIpAddr 			= "1.2.3.4"
	gAppProcessName 	= APPNAME_GATEWAY

)

type fakeDbOpImpl struct {

}

func newFakeDbOpImpl() *fakeDbOpImpl {
	return &fakeDbOpImpl{}
}

func (impl *fakeDbOpImpl) getTableAppData() (ret []TableApp, err error) {
	return []TableApp{TableApp{
		ClusterId: gAppClusterId,
		IpAddr: gAppIpAddr,
		ProcessName: gAppProcessName,
		},
	}, nil
}

func (impl *fakeDbOpImpl) getTableGlobalRuleData() (ret []TableGlobalRule, err error) {
	return []TableGlobalRule{
		TableGlobalRule{
			Rule: Rule{
				Name:			ALARMRULE_APP_NOTALIVE,
				Threshold:		0,
				Durable: 		0,
				Count:			1,
				Interval: 		0,
				ReceiverRole: 	"",
				Enable: 		1,
			},
		},
	}, nil
}

func (impl *fakeDbOpImpl) getTableClusterRuleData() (ret []TableClusterRule, err error) {
	return nil, nil
}

func (impl *fakeDbOpImpl) getTableReceiveData() (ret []TableReceiver, err error) {
	return []TableReceiver{
		TableReceiver{
			Erp: 		"test_erp",
			ClusterId: 	gAppClusterId,
			Mail:		"test_mail",
			Tel:		"test_tel",
		},
	}, nil
}

type fakeCacheOpImpl struct {
	cache *cache2go.CacheTable
}

func newFakeCacheOpImpl() *fakeCacheOpImpl {
	return &fakeCacheOpImpl{
		cache2go.Cache("testCache"),
	}
}

func (impl *fakeCacheOpImpl) get(key string) (string, error) {
	item, err := impl.cache.Value(key)
	if err != nil {
		return "", err
	}
	return item.Data().(string), nil
}

func (impl *fakeCacheOpImpl) setex(key, value string, expireTime int64) error {
	impl.cache.Add(key, time.Second * time.Duration(expireTime), value)
	return nil
}

func (impl *fakeCacheOpImpl) exists(key string) error {
	if ok := impl.cache.Exists(key); ok {
		return nil
	} else {
		return errors.New("not exists")
	}
}

func newRequestHeader() *alarmpb2.RequestHeader {
	return &alarmpb2.RequestHeader{
		Type: alarmpb2.AlarmType_APP_HEARTBEAT,
		ClusterId: gAppClusterId,
		IpAddr: gAppIpAddr,
		AppName: gAppProcessName,
	}
}

func newAppHeartbeatRequest() *alarmpb2.AppHeartbeatRequest {
	return &alarmpb2.AppHeartbeatRequest{
		HbIntervalTime: 3,
	}
}

func TestHandle(t *testing.T) {
	s, err := NewAlarmServer2(&conf)
	if err != nil {
		t.Fatalf("new alarm server error: %v", err)
	}

	s.dbOpImpl = newFakeDbOpImpl()
	s.cacheOpImpl = newFakeCacheOpImpl()

	// test HandleAppHeartbeat
	header := newRequestHeader()
	hbReq := newAppHeartbeatRequest()
	resp, err := s.handleAppHeartbeat(header, hbReq)
	if len(resp.GetHeader().GetError()) != 0 || resp.GetHeader().GetCode() != int64(alarmpb2.AlarmResponseCode_ALARM_OK) {
		t.Fatalf("handle app heartbeat failed: header error: %v", resp.GetHeader().GetError())
	}
	if err != nil {
		t.Fatalf("handle app heartbeat failed: %v", err)
	}
	for i := 0; i < 7; i++ { // check cache2go expire
		time.Sleep(time.Second)
		fmt.Println("exists: ", s.cacheOpImpl.exists("app_not_alive_gateway_12_1.2.3.4"))
	}

	// test db op
	appData, err := s.dbOpImpl.getTableAppData()
	if err != nil {
		t.Fatalf("db op getTableAppData failed: %v", err)
	}
	fmt.Println("getTableAppData: ", appData)

	globalRule, err := s.dbOpImpl.getTableGlobalRuleData()
	if err != nil {
		t.Fatalf("db op getTableGlobalRuleData failed: %v", err)
	}
	fmt.Println("getTableGlobalRuleData: ", globalRule)

	clusterRule, err := s.dbOpImpl.getTableClusterRuleData()
	if err != nil {
		t.Fatalf("db op getTableClusterRuleData failed: %v", err)
	}
	fmt.Println("getTableClusterRuleData: ", clusterRule)

	receiver, err := s.dbOpImpl.getTableReceiveData()
	if err != nil {
		t.Fatalf("db op getTableReceiverData failed: %v", err)
	}
	fmt.Println("getTableReceiver: ", receiver)

	// test HandleRuleAlarm
	//pull db
	s.tableAppPulling()
	s.tableGlobalRulePulling()
	s.tableClusterRulePulling()
	s.tableReceiverPulling()

	if _, err := s.handleRuleAlarm(&alarmpb2.RequestHeader{
		ClusterId: gAppClusterId,
		IpAddr: gAppIpAddr,
		AppName: gAppProcessName,
	}, &alarmpb2.RuleAlarmRequest{
		RuleName: ALARMRULE_APP_NOTALIVE,
		AlarmValue: 1,
		CmpType: alarmpb2.AlarmValueCompareType_GREATER_THAN,
		Remark: []string{fmt.Sprintf("app name: %v", gAppProcessName)},
	}); err != nil {
		t.Fatalf("handleRuleAlarm failed: %v", err)
	}

	to := time.NewTimer(3*time.Second)
	for {
		select {
		case <-to.C:
			t.Fatalf("no alarm message in report queue")
		case msg := <-s.reportQueue:
			fmt.Printf("report message: +%v", msg)
		}
	}

	//exitTime := time.NewTimer(10*time.Second)
	//for {
	//	select {
	//	case <-exitTime.C:
	//		s.context.Done()
	//	}
	//}
}