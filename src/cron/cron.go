package cron

import (
	"flo/assessment/config/cronjob"
	"flo/assessment/config/log"
	"flo/assessment/config/worker"
	"flo/assessment/src/service"
	"flo/assessment/src/tools"
	"fmt"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

type CronJobImpl struct {
}

func MockDataIngestion() {
	// mimic data ingestion pipeline, frequent and big file size
	_cron := cronjob.GetCJ()

	_cron.AddFunc("@every 5s", func() {
		defer func() {
			if r := recover(); r != nil {
				log.Logger.Error("Recovered from panic in cron job", zap.Any("panic", r))
			}
		}()

		log.Logger.Info("CSV data ingestion triggered", zap.Time("timestamp", time.Now().UTC()))

		// Generate unique filenames with UUID
		uuidStr := tools.NewUuid()
		files := []struct {
			interval int
			name     string
		}{
			// {5, fmt.Sprintf("nem12_5min_%s.csv", uuidStr)},
			// {15, fmt.Sprintf("nem12_15min_%s.csv", uuidStr)},
			{30, fmt.Sprintf("nem12_30min_%s.csv", uuidStr)},
		}

		for _, f := range files {
			f := f
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Logger.Error("Recovered from panic in file processing", zap.Any("panic", r), zap.String("file", f.name))
					}
				}()
				// if err := tools.GenerateNEM12Normal(f.name, f.interval, 50, 50); err != nil {
				// 	log.Logger.Error("Failed to generate CSV", zap.String("file", f.name), zap.Error(err))
				// 	continue
				// }
				if err := tools.GenerateNEM12Malformed(f.name, f.interval, 50, 50); err != nil {
					log.Logger.Error("Failed to generate CSV", zap.String("file", f.name), zap.Error(err))
					return
				}
				validationErr, err := service.IFileCheckerService.CheckNEMFile(f.name)
				if err != nil {
					log.Logger.Error("failed to check file", log.Any("path", f.name))
					return
				}

				if len(validationErr) > 0 {
					log.Logger.Error("file is not valid", zap.String("path", f.name), zap.Any("errors", validationErr))
					return
				}

				absPath, _ := filepath.Abs(f.name)
				worker.EnqueueFile(absPath) // enqueue to worker pool
			}()
		}
	})
}
