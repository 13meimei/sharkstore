package main

import (
	"flag"
	"runtime"

	"proxy/gateway-server/server"
	"util/log"
	"util/gogc"
)

var (
	configFileName = flag.String("config", "", "Usage : -config conf/config.toml")
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU() - 1)

	// load config file
	conf := new(server.Config)
	conf.LoadConfig(configFileName)
	log.InitFileLog(conf.Log.Dir, conf.Log.Module, conf.Log.Level)
	srv, err := server.NewServer(conf)
	if err != nil {
		log.Fatal("init server failed, err[%v]", err)
		return
	}
	// start gc
	go gogc.TickerPrintGCSummary(log.GetFileLogger(), "info")
	// run server
	srv.Run()
	log.Info("gateway server start ")
}