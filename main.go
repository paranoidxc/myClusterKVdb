package main

import (
	"fmt"
	"myredis/config"
	"myredis/lib/logger"
	"myredis/resp/handler"
	"myredis/tcp"
	"os"
)

const configFile string = "redis.conf"

var defaultProperties = &config.ServerProperties{
	Bind: "0.0.0.0",
	Port: 6379,
}

func fileExists(fileName string) bool {
	info, err := os.Stat(fileName)
	return err == nil && !info.IsDir()
}

func main() {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "myredis",
		Ext:        "log",
		TimeFORMAT: "2006-01-02",
	})
	logger.SetDebugMode(true)

	if fileExists(configFile) {
		config.SetupConfig(configFile)
	} else {
		config.Properties = defaultProperties
	}

	/*
		err := tcp.ListenAndServeWithSignal(&tcp.Config{
			Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
		}, tcp.MakeHandler())
	*/

	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, handler.MakeHandler())

	if err != nil {
		fmt.Println(err)
		logger.Error(err)
	}
}
