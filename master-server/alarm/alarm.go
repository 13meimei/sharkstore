package alarm

type AlarmKind int

const (
	Invalid    AlarmKind = iota
	// 任务报警
	TaskAlarm
	// 运行时报警,一般是系统出现严重的bug
	RuntimeAlarm
	// 系统报警，系统错误报警
	SysAlarm
	// 资源报警
	ResourceAlarm
)

type Alarm interface {
	Alarm(clusterId uint64, kind AlarmKind, title,content string) error
	Stop()
}

type AlarmMessage struct {
	Title   string
	Content string
	MailTo  string
	SmsTo   string
}

type EmailMsg struct {
	Title   string `json:"title"`
	Content string `json:"content"`
	To      string `json:"to"`
}

type SmsMsg struct {
	Content string `json:"content"`
	To      string `json:"to"`
}

type AlarmMsg struct {
	Email []*EmailMsg `json:"email"`
	Sms   []*SmsMsg   `json:"sms"`
}