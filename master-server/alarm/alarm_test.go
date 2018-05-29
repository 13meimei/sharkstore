package alarm

import (
	"testing"
	"time"
)

var (
	AlarmUrl = "http://alarmgate.sharkstore.local/alarm"
	Mail = "test@sharkstore.com"
	Sms = "12322"
)

func TestRemoteAlarm(t *testing.T) {
	alarm := NewAlarm(1, AlarmUrl, Mail, Sms)
	defer alarm.Stop()
	//err := alarm.Alarm("test", "alarm")
	//if err != nil {
	//	t.Errorf("alarm err %v", err)
	//	return
	//}
	msg := &AlarmMsg{
		Email: []*EmailMsg{&EmailMsg{Title: "test", Content: "alarm", To: "test@sharkstore.com"}},
		Sms: []*SmsMsg{&SmsMsg{Content: "alarm", To: "123"}},
	}
	err := alarm.send(msg)
	if err != nil {
		t.Errorf("alarm err %v", err)
		return
	}
	time.Sleep(time.Second * 2)
	t.Log("test success!!!")
}
