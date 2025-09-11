package config

import (
	"flo/assessment/config/log"
	"math"
	"strings"
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
		err := fn(db)
		if IsPermanentError(err) {
			// permanent error, don't trip CB
			return nil, nil
		}
		return nil, err // transient errors trip CB
	})
	return err
}

func RetryWithCircuitBreaker(db *gorm.DB, fn func(*gorm.DB) error, maxRetries int) error {
	var lastErr error
	for attempt := range maxRetries {
		lastErr = DBWithCircuitBreaker(db, fn)
		if IsPermanentError(lastErr) {
			log.Logger.Error("Permanent DB error, will not retry", zap.Error(lastErr))
			return lastErr
		}
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

func IsPermanentError(err error) bool {
	if err == nil {
		return false
	}

	msg := err.Error()
	// Add any error patterns that indicate bad data
	if strings.Contains(msg, "Data too long") ||
		strings.Contains(msg, "invalid") ||
		strings.Contains(msg, "cannot be null") {
		return true
	}

	// Otherwise, assume transient error (connection, deadlock, timeout, etc.)
	return false
}
