package main

import (
	"flag"
	"fmt"
	"runtime"

	"proxy/gateway-server/server"
	"util/gogc"
	"util/log"
	"util/ping"
	"math/rand"
	"time"
)

var (
	configFileName = flag.String("config", "", "Usage : -config conf/config.toml")
	printVersion   = flag.Bool("v", false, "Usage : -v")
)

var (
	// BuildVersion should generate from build script
	BuildVersion = "unknown"
	// BuildDate should generate from build script
	BuildDate = "unknown"
)

func main() {
	flag.Parse()

	if *printVersion {
		fmt.Println("Version:", BuildVersion)
		fmt.Println("Build Date:", BuildDate)
		return
	}

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
	// start alive report
	if conf.Alarm.PingInterval <= 0 {
		conf.Alarm.PingInterval = 10
	}
	rand.Seed(time.Now().Unix())
	go ping.Ping(conf.Alarm.Address, int64(conf.Cluster.ID), conf.SqlPort, conf.Alarm.PingInterval + int64(rand.Intn(5)))
	// run server
	srv.Run()
	log.Info("gateway server start ")
}
