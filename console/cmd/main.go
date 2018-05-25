package main

import (
	"os"
	"io"

	"github.com/gin-gonic/gin"

	"util/log"
	"console/service"
	"console/config"
	"console/routers"
)

func initFramework(c *config.Config) {
	gin.DisableConsoleColor()
	gin.SetMode(c.GinMode)
	f, _ := os.Create(c.GinLogFile)
	gin.DefaultWriter = io.MultiWriter(f)
}

func initLog(c *config.Config) {
	log.InitFileLog(c.ProjectLogDir, c.ProjectLogModule, c.ProjectLogLevel)
}

func main() {
	config := config.LoadConfig()

	initLog(config)
	initFramework(config)
	service.InitService(config)

	router := routers.NewRouter(config, service.NewService().GetDb())
	router.StartRouter()
}

