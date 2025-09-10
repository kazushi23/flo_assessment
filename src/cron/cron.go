package cron

import (
	"flo/assessment/config/cronjob"
	"flo/assessment/config/log"
	"time"

	"go.uber.org/zap"
)

type CronJobImpl struct {
}

func CreateBaseCronJob() {
	_cron := cronjob.GetCJ()

	_cron.AddFunc("@every 1m", func() {
		log.Logger.Info("Cron job executed at:", zap.Time("timestamp", time.Now().UTC()))
	})
}
