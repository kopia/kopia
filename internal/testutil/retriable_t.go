// Package testutil contains utilities used in tests.
package testutil

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

const (
	maxRetries   = 5
	initialSleep = 1 * time.Second
	maxSleep     = 10 * time.Second
)

// RetriableT is a wrapper around *testing.T with the same methods that supports retrying tests.
type RetriableT struct {
	*testing.T
	suppressErrors bool
	failedCount    int32
}

func (t *RetriableT) maybeSuppressAndSkip(cnt int32) {
	if t.suppressErrors {
		atomic.AddInt32(&t.failedCount, cnt)
		t.SkipNow()

		return
	}
}

// Fail wraps testing.T.Fail().
func (t *RetriableT) Fail() {
	t.T.Helper()
	t.maybeSuppressAndSkip(1)
	t.T.Fail()
}

// FailNow wraps testing.T.FailNow().
func (t *RetriableT) FailNow() {
	t.T.Helper()
	t.maybeSuppressAndSkip(1)
	t.T.FailNow()
}

// Error wraps testing.T.Error().
func (t *RetriableT) Error(args ...interface{}) {
	t.T.Helper()
	t.maybeSuppressAndSkip(1)
	t.T.Error(args...)
}

// Errorf wraps testing.T.Errorf().
func (t *RetriableT) Errorf(msg string, args ...interface{}) {
	t.T.Helper()
	t.maybeSuppressAndSkip(1)
	t.T.Errorf(msg, args...)
}

// Fatal wraps testing.T.Fatal().
func (t *RetriableT) Fatal(args ...interface{}) {
	t.T.Helper()
	t.maybeSuppressAndSkip(1)
	t.T.Fatal(args...)
}

// Fatalf wraps testing.T.Fatalf().
func (t *RetriableT) Fatalf(msg string, args ...interface{}) {
	t.T.Helper()
	t.maybeSuppressAndSkip(1)
	t.T.Fatalf(msg, args...)
}

// Skip wraps testing.T.Skip().
func (t *RetriableT) Skip(args ...interface{}) {
	t.T.Helper()
	t.maybeSuppressAndSkip(0)
	t.T.Skip(args...)
}

// Skipf wraps testing.T.Skipf().
func (t *RetriableT) Skipf(msg string, args ...interface{}) {
	t.T.Helper()
	t.maybeSuppressAndSkip(0)
	t.T.Skipf(msg, args...)
}

// Retry invokes the provided test multiple tests until it succeeds.
func Retry(t *testing.T, testFun func(t *RetriableT)) {
	nextSleepTime := initialSleep

	for att := 1; att <= maxRetries; att++ {
		if att > 1 {
			t.Logf("Sleeping %v before running attempt #%v", nextSleepTime, att)
			time.Sleep(nextSleepTime)
		}

		nextSleepTime += nextSleepTime
		if nextSleepTime > maxSleep {
			nextSleepTime = maxSleep
		}

		r := &RetriableT{suppressErrors: att < maxRetries}

		t.Run(fmt.Sprintf("Attempt-%v", att), func(t2 *testing.T) {
			r.T = t2
			testFun(r)
		})

		if r.failedCount == 0 {
			return
		}
	}
}
