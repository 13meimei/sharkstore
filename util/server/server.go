package server

import (
	"net/http"
	"strings"
	"sync/atomic"
	"fmt"
	"io"
	"os"
	"net"
	"net/http/pprof"

	"util/gogc"
	"util/log"
	"golang.org/x/net/netutil"
)

type ServerConfig struct {
	Ip      string
	Port    string
	// replace ip + port
	Sock    string
	Version string
	ConnLimit   int
}

type ServiceHttpHandler func( /*w*/ http.ResponseWriter /*r*/, *http.Request)

type Stats struct {
	Rangeid            uint64
	Calls              uint64
	Microseconds       uint64
	InputBytes         uint64
	InputMicroseconds  uint64
	OutputBytes        uint64
	OutputMicroseconds uint64
}

// Server is a http server
type Server struct {
	name    string
	sock    string
	version string

	connLimit int
	l      net.Listener
	closed int64

	// handles.
	handlers  map[string]ServiceHttpHandler
}

// NewServer creates the server with given configuration.
func NewServer() *Server {
	return &Server{name: "", handlers: nil}
}

func (s *Server) Init(name string, config *ServerConfig) {
	if config == nil {
		panic("invalid server config")
	}
	s.name = name
	s.sock = config.Ip + ":" + config.Port
	if config.Sock != "" {
		s.sock = config.Sock
	}
	s.version = config.Version
	s.connLimit = config.ConnLimit
	s.handlers = make(map[string]ServiceHttpHandler, 10)

	s.Handle("/debug/ping", ServiceHttpHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
		return
	}))
	s.Handle("/debug/pprof/", ServiceHttpHandler(DebugPprofHandler))
	s.Handle("/debug/pprof/cmdline", ServiceHttpHandler(DebugPprofCmdlineHandler))
	s.Handle("/debug/pprof/profile", ServiceHttpHandler(DebugPprofProfileHandler))
	s.Handle("/debug/pprof/symbol", ServiceHttpHandler(DebugPprofSymbolHandler))
	s.Handle("/debug/pprof/trace", ServiceHttpHandler(DebugPprofTraceHandler))
	s.Handle("/debug/gc", ServiceHttpHandler(func(w http.ResponseWriter, r *http.Request) {
		gogc.PrintGCSummary(w)
	    return
	}))
}

func (s *Server) Handle(name string, handler ServiceHttpHandler) {
	if _, ok := s.handlers[name]; ok {
		panic("duplicate register http handler")
	}
	s.handlers[name] = handler
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.closed, 0, 1) {
		// server is already closed
		return
	}

	log.Info("closing server")
	if s.l != nil {
		s.l.Close()
	}

	log.Info("close server")
}

// isClosed checks whether server is closed or not.
func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.closed) == 1
}

// Run runs the server.
func (s *Server) Run() {
	var l net.Listener
	var err error
	l, err = net.Listen("tcp", s.sock)
	if err != nil {
		log.Fatal("Listen: %v", err)
	}
	if s.connLimit > 0 {
		l = netutil.LimitListener(l, s.connLimit)
	}

	err = http.Serve(l, s)
	if err != nil {
		log.Fatal("http.listenAndServe failed: %s", err.Error())
	}
	s.l = l
	return
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Before hijacke any connection, make sure the ms is initialized.
	if s.isClosed() {
		return
	}

	method := s.getReqMethod(r.RequestURI)
	handler := s.handlers[method]
	if handler == nil {
		if strings.HasPrefix(method, "/debug/pprof/") {
			handler = s.handlers["/debug/pprof/"]
		}
		if handler == nil {
			handler = NotFoundHandler()
		}
	}

	handler(w, r)

	return
}

func (s *Server) Name() string {
	return s.name
}

func (s *Server) getReqMethod(url string) string {
	var invalidMethod string
	index := strings.Index(url, "?")
	if index <= 0 {
		index = len(url)
	}
	method := url[0:index]
	if len(method) <= 0 {
		log.Warn("check method failed:handle[%s]", method)
		return invalidMethod
	}
	return method
}

// Helper handlers

// Error replies to the request with the specified error message and HTTP code.
// It does not otherwise end the request; the caller should ensure no further
// writes are done to w.
// The error message should be plain text.
func Error(w http.ResponseWriter, error string, code int) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	fmt.Fprintln(w, error)
}

// NotFound replies to the request with an HTTP 404 not found error.
func NotFound(w http.ResponseWriter, r *http.Request) { Error(w, "404 page not found", http.StatusNotFound) }

// NotFoundHandler returns a simple request handler
// that replies to each request with a ``404 page not found'' reply.
func NotFoundHandler() ServiceHttpHandler { return ServiceHttpHandler(NotFound) }

type ResponseWriter struct {
	http.ResponseWriter
	writer io.Writer
}

func NewResponseWriter(w http.ResponseWriter, writer io.Writer) *ResponseWriter {
	return &ResponseWriter{ResponseWriter:w, writer: writer }
}

func (w *ResponseWriter) Write(b []byte) (int, error) {
	if w.writer == nil {
		return w.Write(b)
	} else  {
		return w.writer.Write(b)
	}
}

func DebugPprofHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("fbase-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Index(ww, r)
}

func DebugPprofCmdlineHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("fbase-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Cmdline(ww, r)
}

func DebugPprofProfileHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("fbase-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Profile(ww, r)
}

func DebugPprofSymbolHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("fbase-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Symbol(ww, r)
}

func DebugPprofTraceHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("fbase-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Trace(ww, r)
}