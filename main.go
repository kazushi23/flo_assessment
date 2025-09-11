package main

import (
	"flo/assessment/config/log"
	_ "flo/assessment/config/mysql"
	"flo/assessment/config/toml"
	"flo/assessment/config/worker"
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
	fmt.Println(toml.GetConfig().Process.Numworkers, toml.GetConfig().Process.Jobqueuesize, toml.GetConfig().Process.Concurrency)
	// Start worker pool
	numWorkers := toml.GetConfig().Process.Numworkers          // number of worker goroutines
	jobQueueSize := toml.GetConfig().Process.Jobqueuesize      // max pending jobs
	concurrencyPerFile := toml.GetConfig().Process.Concurrency // concurrency inside each file processing
	worker.StartWorkerPool(numWorkers, jobQueueSize, concurrencyPerFile)
	worker.StartAutoRequeue(1)

	// Start cron jobs safely
	tools.NewPanicGroup().Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Logger.Error("Recovered panic in cron job", zap.Any("panic", r))
			}
		}()
		cron.MockDataIngestion()
	})

	r := gin.Default()
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
			"time":   time.Now(),
		})
	})
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
