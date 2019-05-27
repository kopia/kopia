// Package retry implements exponential retry policy.
package retry

import (
	"fmt"
	"time"

	"github.com/kopia/kopia/internal/repologging"
)

var log = repologging.Logger("repo/retry")

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
func WithExponentialBackoff(desc string, attempt AttemptFunc, isRetriableError IsRetriableFunc) (interface{}, error) {
	sleepAmount := retryInitialSleepAmount
	for i := 0; i < maxAttempts; i++ {
		v, err := attempt()
		if !isRetriableError(err) {
			return v, err
		}
		log.Debugf("got error %v when %v (#%v), sleeping for %v before retrying", err, desc, i, sleepAmount)
		time.Sleep(sleepAmount)
		sleepAmount *= 2
		if sleepAmount > retryMaxSleepAmount {
			sleepAmount = retryMaxSleepAmount
		}
	}

	return nil, fmt.Errorf("unable to complete %v despite %v retries", desc, maxAttempts)
}
