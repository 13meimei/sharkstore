package alarm2

import (
	"fmt"
	"strings"
	"net/http"
	"context"
	"encoding/json"

	"util/log"
	"model/pkg/alarmpb2"
)


type alarmMessage struct {
	clusterId 		int64
	appName 		string
	ipAddr 			string
	alarmpb2.RuleAlarmRequest
	ruleThreshold 	float64
}

type reportMessage struct {
	Title 	string 		`json:"title"`
	Content string 		`json:"content"`
	MailTo 	string 		`json:"mailTo"`
	SmsTo 	string 		`json:"smsTo"`
}

func (s *Server) ruleAlarmReportAppend(clusterId int64, appName, ipAddr string, req *alarmpb2.RuleAlarmRequest, ruleThreshlod float64) {
	log.Debug("rule alarm report append cluster id[%v] app name[%v] ip addr[%v] trigger rule name[%v]",
		clusterId, appName, ipAddr, req.GetRuleName())
	s.reportQueue <-alarmMessage{
		clusterId: 	clusterId,
		appName: 	appName,
		ipAddr: 	ipAddr,
		RuleAlarmRequest: *req,
		ruleThreshold: ruleThreshlod,
	}
}

func (s *Server) ruleAlarmReportCron() {
	ctx, cancel := context.WithCancel(s.context)
	defer cancel()

	for {
		select {
		case msg := <-s.reportQueue:
			if err := s.report(msg); err != nil {
				log.Error("alarm report error: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) getReportReceivers(msg alarmMessage) ([]string, []string, error) {
	var mail 	[]string
	var sms 	[]string
	// system receiver
	systemRecvers := s.getMapClusterReceiver(0)
	if len(systemRecvers) == 0 {
		log.Warn("no system receiver")
	} else {
		for _, r := range systemRecvers {
			mail = append(mail, r.Mail)
			sms = append(sms, r.Tel)
		}
	}
	// cluster receiver
	clusterRecvers := s.getMapClusterReceiver(msg.clusterId)
	if len(clusterRecvers) == 0 {
		log.Warn("no cluster[%v] receiver", msg.clusterId)
	} else {
		for _, r := range clusterRecvers {
			mail = append(mail, r.Mail)
			sms = append(sms, r.Tel)
		}
	}

	if len(mail) == 0 || len(sms) == 0 {
		return nil, nil, fmt.Errorf("cluster[%v] no receiver", msg.clusterId)
	}
	return mail, sms, nil
}

func genAlarmContent(msg alarmMessage) (content string) {
	content = fmt.Sprintf("ClusterId[%v] AppName[%v] IpAddr[%v] Trigger RuleName[%v] AlarmValue[%v] CompareType[%v] AlarmThreshold[%v]<br>",
		msg.clusterId, msg.appName, msg.ipAddr, msg.GetRuleName(), msg.GetAlarmValue(), msg.GetCmpType().String(), msg.ruleThreshold)

	for _, r := range msg.GetRemark() {
		content += fmt.Sprintf(" Remark[%v]<br>", r)
	}
	return
}

func (s *Server) report(msg alarmMessage) error {
	log.Debug("alarm report message: %+v", msg)
	var receiverMail 	[]string
	var receiverSms 	[]string

	// mail sms
	receiverMail, receiverSms, err := s.getReportReceivers(msg)
	if err != nil {
		return err
	}

	// content
	content := genAlarmContent(msg)

	reportMsg := &reportMessage{
		Title: "SHARKSTORE ALARM " + msg.RuleName,
		Content: content,
		MailTo: strings.Join(receiverMail, ","),
		SmsTo: strings.Join(receiverSms, ","),
	}
	log.Debug("alarm report title is: %v", reportMsg.Title)
	log.Debug("alarm report content is: %v", reportMsg.Content)

	data, err := json.Marshal(reportMsg)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", s.conf.AlarmGatewayAddr, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	resp, err := s.reportClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

