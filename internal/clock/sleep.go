package clock

import (
	"context"
	"time"
)

// SleepInterruptibly sleeps for the given amount of time, while also honoring cancellation signal.
// Returns false if canceled, true if slept for the entire duration.
func SleepInterruptibly(ctx context.Context, dur time.Duration) bool {
	select {
	case <-ctx.Done():
		return false

	case <-time.After(dur):
		return true
	}
}
