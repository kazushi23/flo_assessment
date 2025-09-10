package cron

import (
	"context"
	"flo/assessment/config/cronjob"
	"flo/assessment/config/log"
	"flo/assessment/src/service"
	"time"

	"go.uber.org/zap"
)

type CronJobImpl struct {
}

func CreateBaseCronJob() {
	_cron := cronjob.GetCJ()
	// zipPaths := []string{"./nem12_data.csv"}
	zipPaths := []string{"./nem12_data.zip", "./nem12_data.csv"}
	_cron.AddFunc("@every 1m", func() {
		log.Logger.Info("Cron job executed at:", zap.Time("timestamp", time.Now().UTC()))
		// Create a cancellable context with timeout (optional)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()

		// Use concurrency = 5 (adjust as needed)
		opts := service.DefaultProcessorOptions()
		opts.BatchSize = 1000         // your preferred DB batch size
		opts.AllowEmptyReading = true // or false
		serviceImpl := service.NewFileProcessServiceImpl(ctx, 5, opts)

		// Loop through all zip files you want to process
		for _, zipPath := range zipPaths {
			if err := serviceImpl.ProcessZipFile(zipPath); err != nil {
				log.Logger.Error("CSV/NEM12 processing failed", zap.String("zipPath", zipPath), zap.Error(err))
			} else {
				log.Logger.Info("CSV/NEM12 processing completed", zap.String("zipPath", zipPath))
			}
		}
	})
}
