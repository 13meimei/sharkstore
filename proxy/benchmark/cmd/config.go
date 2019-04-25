package main

import (
	"time"

	"github.com/BurntSushi/toml"

	"proxy/gateway-server/server"
	"util/log"
)

const (
	DefaultServerName = "shark-bench"

	DefaultInsertSlowLog = 20 * time.Millisecond
	DefaultUpdateSlowLog = 50 * time.Millisecond
	DefaultSelectSlowLog = 100 * time.Millisecond
	DefaultDeleteSlowLog = 20 * time.Millisecond

	DefaultMaxWorkNum      = 100
	DefaultMaxTaskQueueLen = 10000
	DefaultGrpcPoolSize    = 3
	DefaultGrpcInitWinSize = 64 * 1024

	DefaultMaxRawCount = 10000
)

type BenchMarkConfig struct {
	Type    int    `toml:"type,omitempty" json:"type"`
	DataLen int    `toml:"data-len,omitempty" json:"data-len"`
	SendNum int    `toml:"send-num,omitempty" json:"send-num"`
	Threads int    `toml:"threads,omitempty" json:"threads"`
	DB      string `toml:"db,omitempty" json:"db"`
	Table   string `toml:"table,omitempty" json:"table"`
	Batch   int    `toml:"batch,omitempty" json:"batch"`
	Scope   int    `toml:"scope,omitempty" json:"scope"`
}

type Config struct {
	MaxLimit   uint64 `toml:"max-record-limit,omitempty" json:"max-record-limit"`
	AggrEnable bool   `toml:"aggr-func,omitempty" json:"aggr-func"`

	Performance server.PerformConfig `toml:"performance,omitempty" json:"performance"`
	Cluster     server.ClusterConfig `toml:"cluster,omitempty" json:"cluster"`
	Log         server.LogConfig     `toml:"log,omitempty" json:"log"`

	BenchConfig BenchMarkConfig `toml:"benchmark,omitempty" json:"benchmark"`
}

const DefaultConfig = `
# Bench Configuration.

#max limit number for row record
max-record-limit = 10000
aggr-func = true

[performance]
#proxy concurrency number
max-work-num = 100
#task queue size
max-task-queue-len = 10000
#keep connect size for each ds
grpc-pool-size = 10
# 128 KB
grpc-win-size = 131072
#be identified to 'slow command' when time consuming is greater than the value, unit: millisecond
slow-insert = "20ms"
slow-select = "100ms"
slow-delete = "20ms"

[cluster]
id = 1
address = ["127.0.165.52:8887"]


[log]
dir = "./log"
module = "shark-bench"
# log level debug, info, warn, error
level = "debug"
`

var configFileN *string

func (c *Config) LoadConfig(configFileName *string) {
	if *configFileName != "" {
		configFileN = configFileName
		_, err := toml.DecodeFile(*configFileName, c)
		if err != nil {
			log.Panic("load config file failed, err %v", err)
		}
	} else {
		//load default config
		if _, err := toml.Decode(DefaultConfig, c); err != nil {
			log.Panic("decode default config failed, err %v", err)
		}
	}
	if err := c.adjust(); err != nil {
		log.Panic("validate config failed, err %v", err)
	}
}

func (c *Config) adjust() error {
	var err error
	err = c.Performance.Adjust()
	if err != nil {
		return err
	}

	err = c.Cluster.Adjust()
	if err != nil {
		return err
	}

	err = c.Log.Adjust(DefaultServerName)
	if err != nil {
		return err
	}

	return nil
}
