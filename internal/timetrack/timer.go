// Package timetrack tracks the progress and estimates completion of a task.
package timetrack

import (
	"time"
)

// Timer measures elapsed time of operations.
type Timer struct {
	startTime time.Time
}

// Elapsed returns time elapsed since the timer was started.
func (t Timer) Elapsed() time.Duration {
	return time.Since(t.startTime) //nolint:forbidigo
}

// StartTimer starts the timer.
func StartTimer() Timer {
	return Timer{time.Now()} //nolint:forbidigo
}
