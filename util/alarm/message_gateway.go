package alarm

import (
	"net/http"
	"strings"
	"encoding/json"
	"context"
	"util/log"
	"time"
	"errors"
	"fmt"
)


const (
	MESSAGE_LEVEL_INFO 		= iota
	MESSAGE_LEVEL_WARN
	MESSAGE_LEVEL_ERROR
)

type Message struct {
	ClusterId int64
	Title string
	Content string
	Level int
}

type MessageTo struct {
	Title string 		`json:"title"`
	Content string 		`json:"content"`
	MailTo string 		`json:"mailTo"`
	SmsTo string 		`json:"smsTo"`
}

type MessageGateway struct {
	address string
	client *http.Client

	messages chan Message
	receiver MessageReceiver

	ctx context.Context
}

const (
	MESSAGEGATEWAY_WAIT_QUEUE_LENGTH = 10000
)

func NewMessageGateway(ctx context.Context, addr string, receiver MessageReceiver) *MessageGateway {
	gw := &MessageGateway{
		address: addr,
		client: &http.Client{
			Timeout: time.Second,
		},
		messages: make(chan Message, MESSAGEGATEWAY_WAIT_QUEUE_LENGTH),
		receiver: receiver,
		ctx: ctx,
	}

	go gw.wait()
	return gw
}

func (gw *MessageGateway) send_(msg string) error {
	req, err := http.NewRequest("post", gw.address, strings.NewReader(msg))
	if err != nil {
		return err
	}
	req.Header.Set("len", fmt.Sprint(len(msg)))
	resp, err := gw.client.Do(req)
	//_, err = gw.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (gw *MessageGateway) send(title, content string, mailTo, smsTo[]string) error {
	//fmt.Printf("title: %v, content: %v, mailTo: %v, smsTo: %v\n", title, content, mailTo, smsTo)
	msgTo := &MessageTo{
		Title: title,
		Content: content,
		MailTo: strings.Join(mailTo, ","),
		SmsTo: strings.Join(smsTo, ","),
	}

	data, _ := json.Marshal(msgTo) // todo template deal with
	return gw.send_(string(data))
}

func (gw *MessageGateway) wait() {
	for {
		select {
		case msg := <-gw.messages:
			mailTo := gw.receiver.GetMailList(msg.ClusterId, msg.Level)
			smsTo := gw.receiver.GetSmsList(msg.ClusterId, msg.Level)
			log.Info("message: %v, mail to: %v, sms to: %v", msg, mailTo, smsTo)
			if err := gw.send(msg.Title, msg.Content, mailTo, smsTo); err != nil {
				log.Error("alarm message send: %v", err)
			}
		case <-gw.ctx.Done():
			return
		}
	}
}

func (gw *MessageGateway) notify(message Message, timeout time.Duration) (err error) {
	t := time.NewTimer(timeout)
	select {
	case gw.messages <-message:
	case <-t.C:
		err = errors.New("message notify timeout")
	case <-gw.ctx.Done():
		err = errors.New("message notify is canceled")
	}
	return
}
