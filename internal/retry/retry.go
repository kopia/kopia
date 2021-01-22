// Package retry implements exponential retry policy.
package retry

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.GetContextLoggerFunc("retry")

var (
	maxAttempts             = 10
	retryInitialSleepAmount = 1 * time.Second
	retryMaxSleepAmount     = 32 * time.Second
)

// AttemptFunc performs an attempt and returns a value (optional, may be nil) and an error.
type AttemptFunc func() (interface{}, error)

// IsRetriableFunc is a function that determines whether an error is retriable.
type IsRetriableFunc func(err error) bool

// WithExponentialBackoff runs the provided attempt until it succeeds, retrying on all errors that are
// deemed retriable by the provided function. The delay between retries grows exponentially up to
// a certain limit.
func WithExponentialBackoff(ctx context.Context, desc string, attempt AttemptFunc, isRetriableError IsRetriableFunc) (interface{}, error) {
	return internalRetry(ctx, desc, attempt, isRetriableError, retryInitialSleepAmount, retryMaxSleepAmount, maxAttempts, 1.5)
}

// Periodically runs the provided attempt until it succeeds, waiting given fixed amount between attempts.
func Periodically(ctx context.Context, interval time.Duration, count int, desc string, attempt AttemptFunc, isRetriableError IsRetriableFunc) (interface{}, error) {
	return internalRetry(ctx, desc, attempt, isRetriableError, interval, interval, count, 1)
}

// PeriodicallyNoValue runs the provided attempt until it succeeds, waiting given fixed amount between attempts.
func PeriodicallyNoValue(ctx context.Context, interval time.Duration, count int, desc string, attempt func() error, isRetriableError IsRetriableFunc) error {
	_, err := Periodically(ctx, interval, count, desc, func() (interface{}, error) {
		return nil, attempt()
	}, isRetriableError)

	return err
}

// internalRetry runs the provided attempt until it succeeds, retrying on all errors that are
// deemed retriable by the provided function. The delay between retries grows exponentially up to
// a certain limit.
func internalRetry(ctx context.Context, desc string, attempt AttemptFunc, isRetriableError IsRetriableFunc, initial, max time.Duration, count int, factor float64) (interface{}, error) {
	sleepAmount := initial

	for i := 0; i < count; i++ {
		v, err := attempt()
		if err == nil {
			return v, nil
		}

		if !isRetriableError(err) {
			return v, err
		}

		log(ctx).Debugf("got error %v when %v (#%v), sleeping for %v before retrying", err, desc, i, sleepAmount)
		time.Sleep(sleepAmount)
		sleepAmount = time.Duration(float64(sleepAmount) * factor)

		if sleepAmount > max {
			sleepAmount = max
		}
	}

	return nil, errors.Errorf("unable to complete %v despite %v retries", desc, count)
}

// WithExponentialBackoffNoValue is a shorthand for WithExponentialBackoff except the
// attempt function does not return any value.
func WithExponentialBackoffNoValue(ctx context.Context, desc string, attempt func() error, isRetriableError IsRetriableFunc) error {
	_, err := WithExponentialBackoff(ctx, desc, func() (interface{}, error) {
		return nil, attempt()
	}, isRetriableError)

	return err
}

// Always is a retry function that retries all errors.
func Always(err error) bool {
	return true
}

// Never is a retry function that never retries.
func Never(err error) bool {
	return false
}
