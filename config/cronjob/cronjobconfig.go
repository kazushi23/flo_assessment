package cronjob

import (
	"time"

	"github.com/robfig/cron/v3"
)

var _cj *cron.Cron

func init() {
	// Initialize cron with the UTC timing
	// when calling cron, make sure whatever localtimezone is shifted to UTC
	_cj = cron.New(cron.WithLocation(time.UTC))
	_cj.Start()
}

func GetCJ() *cron.Cron {
	return _cj
}

func StopCJ() {
	_cj.Stop()
}
