package main

import (
	"flo/assessment/config/log"
	_ "flo/assessment/config/mysql"
	"flo/assessment/config/toml"
	"flo/assessment/src/cron"
	"flo/assessment/src/tools"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func main() {

	// Recover panics in main startup
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered panic in main startup:", r)
		}
	}()

	// Initialize logger
	log.InitLogger(toml.GetConfig().Log.Path, toml.GetConfig().Log.Level)

	// Start cron jobs in a panic-safe goroutine
	tools.NewPanicGroup().Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Logger.Error("Recovered panic in cron job", zap.Any("panic", r))
			}
		}()
		cron.CreateBaseCronJob()
	})

	r := gin.Default()
	s := &http.Server{
		Addr:           ":8080",
		Handler:        r,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	err := s.ListenAndServe()
	if nil != err {
		fmt.Println(err)
	}
}
