package alarm

import (
	"fmt"
	"time"
	"bytes"
	"sync"
	"golang.org/x/net/context"
	"net/http"
	"net"
	"runtime"
	"util/log"
	"encoding/json"
	"io/ioutil"
	"errors"
)


var (
	ErrInvalidUrl         = errors.New("invalid url")
	ErrAlarmerBusy        = errors.New("alarmer busy")
	ErrUnExpectedResponse = errors.New("unexpected http response")
)

type RemoteAlarm struct {
	cli     *http.Client
	url     string
	mailTo  string
	smsTo   string

	clusterId uint64
	message chan *AlarmMessage

	wg               sync.WaitGroup
	ctx              context.Context
	cancel           context.CancelFunc
}

func NewRemoteAlarm(clusterId uint64, url, mailTo, smsTo string) *RemoteAlarm {
	timeout := time.Duration(5*time.Second)
	client := &http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (net.Conn, error) {
				deadline := time.Now().Add(timeout * time.Second)
				c, err := net.DialTimeout(netw, addr, time.Second*timeout)
				if err != nil {
					return nil, err
				}
				c.SetDeadline(deadline)
				return c, nil
			},
			ResponseHeaderTimeout: time.Second * 2,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	alarm := &RemoteAlarm{
		mailTo: mailTo,
		smsTo: smsTo,
		url: url,
		clusterId: clusterId,
		message: make(chan *AlarmMessage, 1000),
		cli: client,
		ctx: ctx,
		cancel: cancel}
	alarm.wg.Add(1)
	go alarm.work()
	return alarm
}

func (a *RemoteAlarm) work() {
	defer a.wg.Done()
	for {
		select {
		case <-a.message:
		// TODO open alarm
		//case message := <-a.message:
		//var email []*EmailMsg
		//email = append(email, &EmailMsg{Title: message.Title, Content: message.Content, To: message.MailTo})
		//var sms []*SmsMsg
		//sms = append(sms, &SmsMsg{Content: message.Content, To: message.SmsTo})
		//msg := &AlarmMsg{
		//   Email: email,
		//   Sms: sms,
		//}
		//a.send(msg)
		case <-a.ctx.Done():
			log.Info("alarm stopped: %v", a.ctx.Err())
			return
		}
	}
}

func (a *RemoteAlarm) Stop() {
	a.cancel()
	a.wg.Wait()
}

// TODO send email and sms to admin and user
func (a *RemoteAlarm) send(msg *AlarmMsg) error {
	defer func() {
		if r := recover(); r != nil {
			fn := func() string {
				n := 10000
				var trace []byte
				for i := 0; i < 5; i++ {
					trace = make([]byte, n)
					nbytes := runtime.Stack(trace, false)
					if nbytes < len(trace) {
						return string(trace[:nbytes])
					}
					n *= 2
				}
				return string(trace)
			}
			log.Error("panic:%v", r)
			log.Error("Stack: %s", fn())
			return
		}
	}()
	b, err := json.Marshal(msg)
	if err != nil {
		log.Error("[AlarmByMailSms] Marshal error, [%s]", err.Error())
		return err
	}
	log.Debug("[AlarmByMailSms] Post Request: [%s]", b)
	//req, err := http.NewRequest("POST", a.url, bytes.NewReader(b))
	//if err != nil {
	//	log.Error("new request failed, err[%v]", err)
	//	return err
	//}
	//req.Header.Add("User-Agent", "Jimdb-Message-Sender")
	//req.Header.Set("Content-Type", "application/json;charset=utf-8")
	//req.Header.Set("Accept", "application/json,text/html,text/plain")
	//req.Header.Set("Accept-Charset", "utf-8,GBK")
	var resp *http.Response
	for i := 0; i < 4; i++ {
		for j := 0; j < 4; j++ {
			// 必须每次新创建request，因为body会被读取
			req, err := http.NewRequest("POST", a.url, bytes.NewReader(b))
			if err != nil {
				log.Error("new request failed, err[%v]", err)
				return err
			}
			req.Header.Add("User-Agent", "Jimdb-Message-Sender")
			req.Header.Set("Content-Type", "application/json;charset=utf-8")
			req.Header.Set("Accept", "application/json,text/html,text/plain")
			req.Header.Set("Accept-Charset", "utf-8,GBK")
			resp, err = a.cli.Do(req)
			if err != nil {
				log.Error("[AlarmByMailSms] Post error, [%s]", err.Error())
				time.Sleep(time.Duration(j + 1)*2*time.Second)
				continue
			}
			break
		}
		if err != nil {
			return err
		}
		if resp == nil {
			return ErrUnExpectedResponse
		}
		if resp.StatusCode != http.StatusOK {
			log.Error("[AlarmByMailSms] Post reponse status not ok: [%d]", resp.StatusCode)
			time.Sleep(time.Duration(i + 1)*2*time.Second)
			if resp.Body != nil {
				var errbody []byte
				errbody, err = ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				if err == nil {
					log.Warn("[AlarmByMailSms] http errbody: [%s]", string(errbody))
				}
			}
			continue
		} else {
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Error("[AlarmByMailSms] ReadAll error, [%s]", err.Error())
				return err
			}

			log.Debug("[AlarmByMailSms] Post response body: [%s]", body)
			return nil
		}
	}

	return nil
}

func (a *RemoteAlarm) Alarm(clusterId uint64, kind AlarmKind, title, content string) error {
	if len(a.url) == 0 {
		return ErrInvalidUrl
	}
	// 即不发送告警
	if len(a.mailTo) == 0 && len(a.smsTo) == 0 {
		return nil
	}
	message := fmt.Sprintf("FBASE集群 %d 时间 %s ", a.clusterId, time.Now().String()) + content
	am := &AlarmMessage{
		Title:   title,
		Content: message,
		MailTo:  a.mailTo,
		SmsTo:   a.smsTo,
	}
	select {
	case <-a.ctx.Done():
		log.Info("alarm stopped: %v", a.ctx.Err())
		return nil
	case a.message <- am:
	default:
		log.Error("alarm message queue is full!!!!")
		return ErrAlarmerBusy
	}
	return nil
}
