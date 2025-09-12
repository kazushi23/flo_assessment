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
			return counts.ConsecutiveFailures > 5
		},
	}
	dbCircuitBreaker = gobreaker.NewCircuitBreaker(settings)
}

// Wrap DB calls with circuit breaker => returns error, backoff
func DBWithCircuitBreaker(db *gorm.DB, fn func(*gorm.DB) error) (error, bool) {
	backoff := false
	_, err := dbCircuitBreaker.Execute(func() (interface{}, error) {
		err := fn(db)
		if BackOffError(err) {
			// permanent error, don't trip CB
			log.Logger.Warn("BackOffError encountered in DBWithCircuitBreaker", log.Any("error", err))
			backoff = true
			return nil, nil
		}
		log.Logger.Warn("normal error encountered in DBWithCircuitBreaker", log.Any("error", err))
		return nil, err // transient errors trip CB
	})

	return err, backoff
}

// to be plugged into batch insert statments
func RetryWithCircuitBreaker(db *gorm.DB, fn func(*gorm.DB) error, maxRetries int) (error, bool) {
	var lastErr error
	var backoff bool
	for attempt := range maxRetries {
		lastErr, backoff = DBWithCircuitBreaker(db, fn)
		if backoff {
			// Don't retry, don't trip CB, just propagate
			return nil, true
		}
		if lastErr == nil {
			return nil, false
		}
		log.Logger.Warn("DB operation failed, will retry", zap.Int("attempt", attempt+1), zap.Int("max_retries", maxRetries), zap.Error(lastErr))
		sleep := time.Duration(math.Pow(2, float64(attempt))) * time.Second
		time.Sleep(sleep)
	}
	log.Logger.Error("DB operation failed after max retry", zap.Int("max_retries", maxRetries), zap.Error(lastErr))

	return lastErr, false
}

// if errors fall in this categories, dont retry
func BackOffError(err error) bool {
	if err == nil {
		return false
	}

	msg := err.Error()
	// Add any error patterns that indicate bad data
	if strings.Contains(msg, "Data too long") ||
		strings.Contains(msg, "invalid") ||
		strings.Contains(msg, "cannot be null") ||
		strings.Contains(msg, "Error 1213 (40001)") {
		return true
	}

	// Otherwise, assume transient error (connection, timeout, etc.)
	return false
}
