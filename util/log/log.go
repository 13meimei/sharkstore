package log

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
)

const (
	// A colon appears after these items:  2009/01/23 01:23:23.123123 /a/b/c/d.go:23: message
	Ldate         = 1 << iota     // the date: 2009/01/23
	Ltime                         // the time: 01:23:23
	Lmicroseconds                 // microsecond resolution: 01:23:23.123123.  assumes Ltime.
	Llongfile                     // full file name and line number: /a/b/c/d.go:23
	Lshortfile                    // final file name element and line number: d.go:23. overrides Llongfile
	LstdFlags     = Ldate | Ltime // initial values for the standard logger

	LogFileNameDateFormat = "2006-01-02"
)

var (
	errLogFileName   = "_err.log"
	warnLogFileName  = "_warn.log"
	infoLogFileName  = "_info.log"
	debugLogFileName = "_debug.log"
)

type logWriter struct {
	mu     sync.Mutex     // ensures atomic writes; protects the following fields
	prefix string         // prefix to write at beginning of each line
	flag   int            // properties
	out    io.WriteCloser // destination for output
	buf    []byte         // for accumulating text to write
}

func newLogWriter(out io.WriteCloser, prefix string, flag int) *logWriter {
	return &logWriter{out: out, prefix: prefix, flag: flag}
}

func itoa(buf *[]byte, i int, wid int) {
	var u uint = uint(i)
	if u == 0 && wid <= 1 {
		*buf = append(*buf, '0')
		return
	}
	// Assemble decimal in reverse order.
	var b [32]byte
	bp := len(b)
	for ; u > 0 || wid > 0; u /= 10 {
		bp--
		wid--
		b[bp] = byte(u%10) + '0'
	}
	*buf = append(*buf, b[bp:]...)
}

func (l *logWriter) formatHeader(buf *[]byte, t time.Time, file string, line int) {
	*buf = append(*buf, l.prefix...)
	if l.flag&(Ldate|Ltime|Lmicroseconds) != 0 {
		if l.flag&Ldate != 0 {
			year, month, day := t.Date()
			itoa(buf, year, 4)
			*buf = append(*buf, '-')
			itoa(buf, int(month), 2)
			*buf = append(*buf, '-')
			itoa(buf, day, 2)
			*buf = append(*buf, ' ')
		}
		if l.flag&(Ltime|Lmicroseconds) != 0 {
			hour, min, sec := t.Clock()
			itoa(buf, hour, 2)
			*buf = append(*buf, ':')
			itoa(buf, min, 2)
			*buf = append(*buf, ':')
			itoa(buf, sec, 2)
			if l.flag&Lmicroseconds != 0 {
				*buf = append(*buf, ',')
				itoa(buf, t.Nanosecond()/1e6, 3)
			}
			*buf = append(*buf, ' ')
		}
	}
	if l.flag&(Lshortfile|Llongfile) != 0 {
		if l.flag&Lshortfile != 0 {
			short := file
			for i := len(file) - 1; i > 0; i-- {
				if file[i] == '/' {
					short = file[i+1:]
					break
				}
			}
			file = short
		}
		*buf = append(*buf, file...)
		*buf = append(*buf, ':')
		itoa(buf, line, -1)
		*buf = append(*buf, ": "...)
	}
}

func (l *logWriter) output(s string, file string, line int, now time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.buf = l.buf[:0]
	l.formatHeader(&l.buf, now, file, line)
	l.buf = append(l.buf, s...)
	if len(s) > 0 && s[len(s)-1] != '\n' {
		l.buf = append(l.buf, '\n')
	}
	_, err := l.out.Write(l.buf)
	return err
}

func (lw *logWriter) rotateFile(logDir, logFile, module string, rotate bool) {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	if lw.out != nil {
		lw.out.Close()
	}
	file, err := lw.createFile(logDir, logFile, module, rotate)
	if err != nil {
		file = os.Stdout
	}
	lw.out = file

	if err == nil && logFile == errLogFileName {
		os.Stderr = file
		var crashMsg string
		if err := logCrash(file); err == nil {
			crashMsg = "Log file set up capture system level crash information success.\n"
		} else {
			crashMsg = "Log file set up capture system level crash information fail:\n" + err.Error() + "\n"
		}

		if f, e := file.Stat(); e == nil && f.Size() == 0 {
			// Write header.
			var buf bytes.Buffer
			fmt.Fprintf(&buf, "Log file created at: %s\n", time.Now().Format("2006/01/02 15:04:05"))
			fmt.Fprintf(&buf, crashMsg)
			fmt.Fprintf(&buf, "Log line format: yyyy-mm-dd hh:mm:ss.uuuuuu[DIWE] file:line: msg\n")
			fmt.Fprintf(&buf, "####################################################################\n\n")
			lw.out.Write(buf.Bytes())
		}
	}
}

func (lw *logWriter) createFile(logDir, logFile, module string, rotate bool) (*os.File, error) {
	if _, err := os.Stat(logDir); err != nil && os.IsNotExist(err) {
		if err = os.MkdirAll(logDir, os.ModePerm); err != nil {
			fmt.Printf("[Util.Logger]Create logger dir[%s] err: [%s]\r\n", logDir, err)
		}
	}

	logFileOpt := os.O_RDWR | os.O_CREATE | os.O_APPEND
	logFilePath := logDir + "/" + module + logFile
	if rotate {
		yesterday := time.Now().AddDate(0, 0, -1)
		os.Rename(logFilePath, logFilePath+"."+yesterday.Format(LogFileNameDateFormat))
	}

	file, err := os.OpenFile(logFilePath, logFileOpt, os.ModePerm)
	if err != nil {
		fmt.Printf("[Util.Logger]Create logger file[%s] err: [%s]\r\n", logFilePath, err)
	}
	return file, err
}

const (
	TraceLevel = 0
	DebugLevel = 1
	InfoLevel  = 2
	WarnLevel  = 3
	ErrorLevel = 4
	FatalLevel = 5
)

var levels = []string{
	"[TRACE]",
	"[DEBUG]",
	"[INFO.]",
	"[WARN.]",
	"[ERROR]",
	"[FATAL]",
}

type entity struct {
	msg      string
	now      time.Time
	file     string
	line     int
}

type Log struct {
	dir       string
	module    string
	level     int
	startTime time.Time
	flag      int
	err       *logWriter
	warn      *logWriter
	info      *logWriter
	debug     *logWriter
	entityCh  chan *entity
	quit      chan struct{}
	wg        sync.WaitGroup
}

var glog *Log = NewDefaultLog()

func NewDefaultLog() *Log {
	log, err := NewLog("", "", "DEBUG")
	if err != nil {
		panic(err)
	}
	return log
}

func NewLog(dir, module, level string) (*Log, error) {
	lg := new(Log)
	lg.dir = dir
	lg.module = module
	lg.SetLevel(level)
	if err := lg.initLog(dir, module); err != nil {
		return nil, err
	}
	lg.startTime = time.Now()
	lg.entityCh = make(chan *entity, 204800)
	lg.quit = make(chan struct{})

	if dir != "" {
		go lg.checkLogRotation(dir, module)
	}
	lg.wg.Add(1)
	go lg.loopMsg()

	return lg, nil
}

func InitFileLog(dir, module, level string) {
	log, err := NewLog(dir, module, level)
	if err != nil {
		panic(err)
	}
	glog = log
}

func GetFileLogger() *Log {
	return glog
}

func (l *Log) initLog(logDir, module string) error {
	logOpt := Lshortfile | LstdFlags | Lmicroseconds
	if logDir == "" {
		l.debug = newLogWriter(os.Stdout, "", logOpt)
		l.info = newLogWriter(os.Stdout, "", logOpt)
		l.warn = newLogWriter(os.Stdout, "", logOpt)
		l.err = newLogWriter(os.Stdout, "", logOpt)

		return nil
	}

	if fi, err := os.Stat(logDir); err != nil {
		if pathErr, ok := err.(*os.PathError); ok {
			if os.IsNotExist(pathErr) {
			   os.MkdirAll(logDir, 0755)
			}
		}else{
			return err
		}
	} else if !fi.IsDir() {
		return errors.New(logDir + " is not a directory")
	}
	l.flag = logOpt

	l.debug = newLogWriter(nil, "", logOpt)
	l.info = newLogWriter(nil, "", logOpt)
	l.warn = newLogWriter(nil, "", logOpt)
	l.err = newLogWriter(nil, "", logOpt)
	l.debug.rotateFile(logDir, debugLogFileName, module, false)
	l.info.rotateFile(logDir, infoLogFileName, module, false)
	l.warn.rotateFile(logDir, warnLogFileName, module, false)
	l.err.rotateFile(logDir, errLogFileName, module, false)

	return nil
}

func (l *Log) SetLevel(level string) {
	switch level {
	case "TRACE", "trace", "Trace":
		l.level = TraceLevel
	case "", "debug", "Debug", "DEBUG":
		l.level = DebugLevel
	case "info", "Info", "INFO":
		l.level = InfoLevel
	case "warn", "Warn", "WARN":
		l.level = WarnLevel
	case "error", "Error", "ERROR":
		l.level = ErrorLevel
	default:
		l.level = InfoLevel
	}
}

func (l *Log) SetPrefix(s, level string) string {
	return level + " " + s
}

func (l *Log) IsEnableDebug() bool {
	return l.level <= DebugLevel
}
func (l *Log) IsEnableInfo() bool {
	return l.level <= InfoLevel
}
func (l *Log) IsEnableWarn() bool {
	return l.level <= WarnLevel
}
func (l *Log) IsEnableError() bool {
	return l.level <= ErrorLevel
}

func (l *Log) IsEnableTrace() bool {
	return l.level <= TraceLevel
}

func (l *Log) Output(calldepth int, s string, sync bool) {
	now := time.Now()
	var file string
	var line int
	var ok bool
	if l.flag&(Lshortfile|Llongfile) != 0 {
		_, file, line, ok = runtime.Caller(calldepth)
		if !ok {
			file = "???"
			line = 0
		}
	}
	if sync {
		l.printMsg(s, file, line, now)
	} else {
		l.putMsg(s, file, line, now)
	}
}

func (l *Log) close() {
	close(l.quit)
	l.wg.Wait()
}

func (l *Log) putMsg(msg string, file string, line int, now time.Time) {
	l.entityCh <- &entity{msg: msg, file:file, line:line, now:now}
}

func (l *Log) loopMsg() {
	defer l.wg.Done()
	for {
		select {
		case entity := <-l.entityCh:
			l.printMsg(entity.msg, entity.file, entity.line, entity.now)
			POP_ALL:
			for {
				select {
				case entity := <-l.entityCh:
					l.printMsg(entity.msg, entity.file, entity.line, entity.now)
				default:
					break POP_ALL
				}
			}
		case <-l.quit:
			l.err.out.Close()
			l.warn.out.Close()
			l.info.out.Close()
			l.debug.out.Close()
			return
		}
	}
	//for entity := range l.entityCh {
	//	l.printMsg(entity.msg, entity.file, entity.line, entity.now)
	//}
}

func (l *Log) printMsg(msg string, file string, line int, now time.Time) {
	switch l.level {
	case TraceLevel:
		switch msg[1] {
		case 'I', 'W', 'E', 'F':
			l.debug.output(msg, file, line, now)
		}
	case DebugLevel:
		switch msg[1] {
		case 'I', 'W', 'E', 'F':
			l.debug.output(msg, file, line, now)
		}
	case InfoLevel:
		switch msg[1] {
		case 'W', 'E', 'F':
			l.info.output(msg, file, line, now)
		}
	case WarnLevel:
		switch msg[1] {
		case 'E', 'F':
			l.warn.output(msg, file, line, now)
		}
	}
	switch msg[1] {
	case 'T':
		l.debug.output(msg, file, line, now)
	case 'D':
		l.debug.output(msg, file, line, now)
	case 'I':
		l.info.output(msg, file, line, now)
	case 'W':
		l.warn.output(msg, file, line, now)
	case 'E':
		l.err.output(msg, file, line, now)
	case 'F':
		l.err.output(msg, file, line, now)
	}
}

func (l *Log) checkLogRotation(logDir, module string) {
	for {
		yesterday := time.Now().AddDate(0, 0, -1)
		_, err := os.Stat(logDir + "/" + module + errLogFileName + "." + yesterday.Format(LogFileNameDateFormat))
		if err == nil || time.Now().Day() == l.startTime.Day() {
			time.Sleep(time.Second * 600)
			continue
		}

		//rotate the log files
		l.debug.rotateFile(logDir, debugLogFileName, module, true)
		l.info.rotateFile(logDir, infoLogFileName, module, true)
		l.warn.rotateFile(logDir, warnLogFileName, module, true)
		l.err.rotateFile(logDir, errLogFileName, module, true)
		l.startTime = time.Now()
	}
}

func (l *Log)Debug(format string, v ...interface{}) {
	if l.IsEnableDebug() {
		l.Output(3, l.SetPrefix(fmt.Sprintf(format+"\r\n", v...), levels[DebugLevel]), true)
	}
}

func (l *Log)Info(format string, v ...interface{}) {
	if l.IsEnableInfo() {
		l.Output(3, l.SetPrefix(fmt.Sprintf(format+"\r\n", v...), levels[InfoLevel]), true)
	}
}

func (l *Log)Warn(format string, v ...interface{}) {
	if l.IsEnableWarn() {
		l.Output(3, l.SetPrefix(fmt.Sprintf(format+"\r\n", v...), levels[WarnLevel]), true)
	}
}

func (l *Log)Error(format string, v ...interface{}) {
	l.Output(3, l.SetPrefix(fmt.Sprintf(format+"\r\n", v...), levels[ErrorLevel]), true)
}

func (l *Log)Fatal(format string, v ...interface{}) {
	l.Output(3, l.SetPrefix(fmt.Sprintf(format+"\r\n", v...), levels[FatalLevel]), true)
	l.close()
	os.Exit(1)
}

func (l *Log)Panic(format string, v ...interface{}) {
	s := fmt.Sprintf(format+"\r\n", v...)
	l.Output(3, l.SetPrefix(s, levels[FatalLevel]), true)
	panic(s)
}

func Debug(format string, v ...interface{}) {
	glog.Debug(format, v...)
}


func Info(format string, v ...interface{}) {
	glog.Info(format, v...)
}

func Warn(format string, v ...interface{}) {
	glog.Warn(format, v...)
}

func Error(format string, v ...interface{}) {
	glog.Error(format, v...)
}

func Fatal(format string, v ...interface{}) {
	glog.Fatal(format, v...)
}

func Panic(format string, v ...interface{}) {
	glog.Panic(format, v...)
}

func SetLevel(level string) {
	glog.SetLevel(level)
}

func IsEnableDebug() bool {
	return glog.level <= DebugLevel
}
func IsEnableInfo() bool {
	return glog.level <= InfoLevel
}
func IsEnableWarn() bool {
	return glog.level <= WarnLevel
}
func IsEnableError() bool {
	return glog.level <= ErrorLevel
}

func IsEnableTrace() bool {
	return glog.level <= TraceLevel
}
