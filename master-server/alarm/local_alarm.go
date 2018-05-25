package alarm

type LocalAlarm struct {

}

func NewLocalAlarm() Alarm {
	return &LocalAlarm{}
}

func (l *LocalAlarm) Alarm(clusterId uint64, kind AlarmKind, title, content string) error {
	return nil
}

func (l *LocalAlarm) Stop() {

}
