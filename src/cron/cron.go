package cron

import (
	"flo/assessment/config/cronjob"
	"flo/assessment/config/log"
	"flo/assessment/config/worker"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

type CronJobImpl struct {
}

func CreateBaseCronJob() {
	_cron := cronjob.GetCJ()
	zipPaths := []string{"./nem12_data.zip", "./nem12_data.csv"}
	// zipPaths := []string{"./nem12_data.zip"}

	_cron.AddFunc("@every 1m", func() {
		log.Logger.Info("Cron job triggered", zap.Time("timestamp", time.Now().UTC()))

		for _, zipPath := range zipPaths {
			absPath, _ := filepath.Abs(zipPath)
			worker.EnqueueFile(absPath) // enqueue to worker pool
		}
	})
}

func MockDataIngestion() {
	// mimic data ingestion pipeline, frequent and big file size

	//

}
