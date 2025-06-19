// Package sleepable implements a timer that can trigger at or soon after a given time and supports the computer going to sleep while the timer is waiting.
package sleepable

import (
	"sync"
	"time"
)

// MaxSleepTime is the maximum duration the timer will sleep in one interval. Can be overridden for testing.
var MaxSleepTime = 15 * time.Second

// Timer is a timer that can trigger at or soon after a given time and supports the computer going to sleep while the timer is waiting.
type Timer struct {
	C        <-chan struct{}
	stopChan chan struct{}
	stopOnce sync.Once
}

// Stop stops the timer.
func (c *Timer) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopChan)
	})
}

// NewTimer creates a new timer that will trigger at the given time.
func NewTimer(nowFunc func() time.Time, until time.Time) *Timer {
	ch := make(chan struct{})

	t := &Timer{
		C:        ch,
		stopChan: make(chan struct{}),
	}

	var currentTimer *time.Timer

	// capture maxSleepTime at the start of the goroutine to avoid race conditions.
	maxSleepTime := MaxSleepTime

	// start a goroutine that will sleep until the timer is triggered or the timer is stopped.
	go func() {
		defer func() {
			if currentTimer != nil {
				currentTimer.Stop()
			}
		}()

		for {
			now := nowFunc()

			// when the current time is after the target time, the timer immediately triggers by closing the channel.
			if now.After(until) {
				close(ch)
				return
			}

			nextSleepTime := until.Sub(now)
			if nextSleepTime > maxSleepTime {
				nextSleepTime = maxSleepTime
			}

			if currentTimer != nil {
				currentTimer.Stop()
			}

			currentTimer = time.NewTimer(nextSleepTime)

			select {
			case <-t.stopChan:
				// stop channel was closed, exit without closing the "C" channel
				return

			case <-currentTimer.C:
				// timer did trigger, close the channel to signal that sleepable.Timer.C is done
				close(ch)
				return
			}
		}
	}()

	return t
}
