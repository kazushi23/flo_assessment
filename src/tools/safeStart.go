package tools

import (
	"flo/assessment/config/log"
	"flo/assessment/config/toml"
	"flo/assessment/src/cron"
	"fmt"

	"go.uber.org/zap"
)

// SafeStart initializes logger and cron jobs with panic recovery
func SafeStart() {
	// Recover panics in main startup
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered panic in main startup:", r)
		}
	}()

	// Initialize logger
	log.InitLogger(toml.GetConfig().Log.Path, toml.GetConfig().Log.Level)

	// Start cron jobs in a panic-safe goroutine
	NewPanicGroup().Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Logger.Error("Recovered panic in cron job", zap.Any("panic", r))
			}
		}()
		cron.CreateBaseCronJob()
	})
}
