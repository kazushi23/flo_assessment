package config

import (
	"flo/assessment/config/log"
	"math"
	"time"

	"github.com/sony/gobreaker"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var dbCircuitBreaker *gobreaker.CircuitBreaker

func init() {
	settings := gobreaker.Settings{
		Name:        "DBCircuitBreaker",
		MaxRequests: 5,
		Interval:    30 * time.Second,
		Timeout:     10 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures > 3
		},
	}
	dbCircuitBreaker = gobreaker.NewCircuitBreaker(settings)
}

// Wrap DB calls with circuit breaker
func DBWithCircuitBreaker(db *gorm.DB, fn func(*gorm.DB) error) error {
	_, err := dbCircuitBreaker.Execute(func() (interface{}, error) {
		return nil, fn(db)
	})
	return err
}

func RetryWithCircuitBreaker(db *gorm.DB, fn func(*gorm.DB) error, maxRetries int) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		lastErr = DBWithCircuitBreaker(db, fn)
		if lastErr == nil {
			return nil
		}
		log.Logger.Warn("DB operation failed, will retry", zap.Int("attempt", attempt+1), zap.Int("max_retries", maxRetries), zap.Error(lastErr))
		sleep := time.Duration(math.Pow(2, float64(attempt))) * time.Second
		time.Sleep(sleep)
	}
	log.Logger.Error("DB operation failed after max retry", zap.Int("max_retries", maxRetries), zap.Error(lastErr))

	return lastErr
}
