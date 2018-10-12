package config

import (
	"fmt"

	"github.com/docopt/docopt-go"

	"util/config"
	"util/log"
	"encoding/json"
)

type LoginConfig struct {
	SsoLoginUrl    string
	SsoLogoutUrl   string
	SsoCookieName  string
	SsoDomainName  string
	SsoExcludePath []string
	SsoVerifyUrl   string

	AppDomainName string
	AppUrl        string
	AppName       string
	AppToken      string
}

type ClusterConfig struct {
	Id   int    `json:"id"`
	Addr string `json:"addr"`
}

type Config struct {
	ProjectHomeDir string
	ReqListenPort  int

	// gin
	GinLogFile string
	GinMode    string

	// mysql api
	MysqlHost   string
	MysqlPort   int
	MysqlUser   string
	MysqlPasswd string

	//lock cluster list
	LockClusters []*ClusterConfig
	//configure cluster list
	ConfClusters []*ClusterConfig

	// log
	ProjectLogDir    string
	ProjectLogModule string
	ProjectLogLevel  string

	MonitorDomain string

	*LoginConfig
}

func LoadConfig() *Config {
	const usage = `
Usage:
    console --config=CONFIG_FILE
`
	_, err := docopt.ParseDoc(usage)
	if err != nil {
		panic(fmt.Sprintf("Parse arguments failed. err:[%s]\n", err.Error()))
	}

	config.InitConfig()
	if config.Config == nil {
		panic("Cannot found config file.")
	}

	c := new(Config)
	var found bool
	if c.ProjectHomeDir, found = config.Config.String("project.home.dir"); !found {
		log.Panic("Config project.home.dir not specified")
	}
	if c.ReqListenPort, found = config.Config.Int("http.port"); !found {
		log.Panic("Config http.port not specified")
	}
	if c.GinLogFile, found = config.Config.String("gin.log.file"); !found {
		log.Panic("Config gin.log.file not specified")
	}
	if c.GinMode, found = config.Config.String("gin.mode"); !found {
		log.Panic("Config gin.mode not specified")
	}
	if c.GinMode != "debug" && c.GinMode != "release" && c.GinMode != "test" {
		log.Panic("Invalid gin.mode:" + c.GinMode)
	}

	if c.MysqlHost, found = config.Config.String("mysql.host"); !found {
		log.Panic("Config mysql.host not specified")
	}
	if c.MysqlPort, found = config.Config.Int("mysql.port"); !found {
		log.Panic("Config mysql.port not specified")
	}
	if c.MysqlUser, found = config.Config.String("mysql.user"); !found {
		log.Panic("Config mysql.user not specified")
	}
	if c.MysqlPasswd, found = config.Config.String("mysql.passwd"); !found {
		log.Panic("Config mysql.passwd not specified")
	}

	c.LockClusters, c.ConfClusters = make([]*ClusterConfig, 0), make([]*ClusterConfig, 0)
	var lockInfo, configureInfo string
	if lockInfo, found = config.Config.String("lock.clusters"); !found {
		log.Warn("Config lock.clusters not specified")
	} else {
		err = json.Unmarshal([]byte(lockInfo), &c.LockClusters)
		if err != nil {
			log.Panic("Config lock.clusters resolve error, %v", err)
		}
	}

	if configureInfo, found = config.Config.String("configure.clusters"); !found {
		log.Warn("Config configure.clusters not specified")
	} else {
		err = json.Unmarshal([]byte(configureInfo), &c.ConfClusters)
		if err != nil {
			log.Panic("Config lock.clusters resolve error, %v", err)
		}
	}

	if c.ProjectLogDir, found = config.Config.String("log.dir"); !found {
		log.Panic("Config log.dir not specified")
	}
	if c.ProjectLogModule, found = config.Config.String("log.module"); !found {
		log.Warn("Config log.module not specified, use default console")
		c.ProjectLogModule = "console"
	}
	if c.ProjectLogLevel, found = config.Config.String("log.level"); !found {
		log.Warn("Config log.level not specified, use default info")
		c.ProjectLogLevel = "info"
	}

	if c.MonitorDomain, found = config.Config.String("monitor.domain"); !found {
		log.Warn("Config monitor.domain not specified, cannot show monitor metric")
	}

	c.LoginConfig = new(LoginConfig)

	return c
}
