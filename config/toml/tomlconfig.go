package toml

import (
	"fmt"

	"github.com/spf13/viper"
)

type TomlConfig struct {
	AppName     string
	Environment string
	Log         LogConfig
	Mysql       MysqlConfig
	Redis       RedisConfig
	Cron        CronConfig
	Process     ProcessConfig
}

type LogConfig struct {
	Path  string
	Level string
}

type CronConfig struct{}

type MysqlConfig struct {
	Host     string
	User     string
	Password string
	DbName   string
	Port     int64
}

type RedisConfig struct {
	Urls     []string
	Password string
}

type ProcessConfig struct {
	Concurrency          int
	Jobqueuesize         int
	Numworkers           int
	Maxdbconnections     int
	Maxdbidleconnections int
	Batchsize            int
}

var c TomlConfig // c is type TomlConfig

func init() {
	//viper is used as a configuration solution for Go Applications
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println(err)
	}
	viper.Unmarshal(&c) // from low level format to object (json) structure
}

func GetConfig() TomlConfig {
	return c
}
