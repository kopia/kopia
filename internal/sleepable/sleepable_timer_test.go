package sleepable

import (
	"sync"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/clock"
)

const testMaxSleepTime = 10 * time.Millisecond

// setMaxSleepTimeForTest sets MaxSleepTime to the given duration and registers a cleanup function
// to restore the original value when the test completes.
//
//nolint:unparam
func setMaxSleepTimeForTest(t *testing.T, duration time.Duration) {
	t.Helper()

	originalMaxSleepTime := MaxSleepTime
	MaxSleepTime = duration

	t.Cleanup(func() {
		MaxSleepTime = originalMaxSleepTime
	})
}

func TestNewTimer(t *testing.T) {
	// Set a small MaxSleepTime for testing
	setMaxSleepTimeForTest(t, testMaxSleepTime)

	tests := []struct {
		name     string
		duration time.Duration
		expected time.Duration
	}{
		{
			name:     "short duration",
			duration: 20 * time.Millisecond,
			expected: 20 * time.Millisecond,
		},
		{
			name:     "long duration capped at maxSleepTime",
			duration: 1 * time.Second,
			expected: testMaxSleepTime,
		},
		{
			name:     "exactly maxSleepTime",
			duration: testMaxSleepTime,
			expected: testMaxSleepTime,
		},
		{
			name:     "zero duration",
			duration: 0,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := clock.Now()
			target := start.Add(tt.duration)

			timer := NewTimer(clock.Now, target)

			// Wait for timer to trigger
			<-timer.C

			elapsed := clock.Now().Sub(start)

			if tt.duration == 0 {
				if elapsed > 10*time.Millisecond {
					t.Errorf("zero duration timer took too long: %v", elapsed)
				}

				return
			}

			if tt.duration <= testMaxSleepTime {
				if elapsed < tt.duration-5*time.Millisecond || elapsed > tt.duration+50*time.Millisecond {
					t.Errorf("timer triggered at wrong time: expected ~%v, got %v", tt.duration, elapsed)
				}
			} else {
				if elapsed < testMaxSleepTime-5*time.Millisecond || elapsed > testMaxSleepTime+50*time.Millisecond {
					t.Errorf("long timer triggered at wrong time: expected ~%v, got %v", testMaxSleepTime, elapsed)
				}
			}
		})
	}
}

func TestTimerStop(t *testing.T) {
	// Set a small MaxSleepTime for testing
	setMaxSleepTimeForTest(t, testMaxSleepTime)

	t.Run("stop before trigger", func(t *testing.T) {
		start := clock.Now()
		target := start.Add(100 * time.Millisecond)

		timer := NewTimer(clock.Now, target)
		timer.Stop()
		time.Sleep(20 * time.Millisecond)
		select {
		case <-timer.C:
			t.Error("timer triggered after being stopped")
		default:
		}
	})

	t.Run("stop after trigger", func(t *testing.T) {
		start := clock.Now()
		target := start.Add(10 * time.Millisecond)

		timer := NewTimer(clock.Now, target)
		<-timer.C
		timer.Stop()
	})
}

func TestTimerConcurrentStop(t *testing.T) {
	// Set a small MaxSleepTime for testing
	setMaxSleepTimeForTest(t, testMaxSleepTime)

	t.Run("multiple stops", func(t *testing.T) {
		start := clock.Now()
		target := start.Add(100 * time.Millisecond)

		timer := NewTimer(clock.Now, target)

		var wg sync.WaitGroup
		for range 10 {
			wg.Add(1)

			go func() {
				defer wg.Done()
				timer.Stop()
			}()
		}

		wg.Wait()
		time.Sleep(20 * time.Millisecond)
		select {
		case <-timer.C:
			t.Error("timer triggered after being stopped")
		default:
		}
	})
}

func TestTimerEdgeCases(t *testing.T) {
	// Set a small MaxSleepTime for testing
	setMaxSleepTimeForTest(t, testMaxSleepTime)

	t.Run("past time", func(t *testing.T) {
		start := clock.Now()
		target := start.Add(-1 * time.Second)
		timer := NewTimer(clock.Now, target)
		select {
		case <-timer.C:
		case <-time.After(10 * time.Millisecond):
			t.Error("timer did not trigger immediately for past time")
		}
	})

	t.Run("exactly now", func(t *testing.T) {
		start := clock.Now()
		target := start
		timer := NewTimer(clock.Now, target)
		select {
		case <-timer.C:
		case <-time.After(10 * time.Millisecond):
			t.Error("timer did not trigger immediately for current time")
		}
	})

	t.Run("very long duration", func(t *testing.T) {
		start := clock.Now()
		target := start.Add(24 * time.Hour)
		timer := NewTimer(clock.Now, target)
		select {
		case <-timer.C:
			elapsed := clock.Now().Sub(start)
			if elapsed < testMaxSleepTime-5*time.Millisecond || elapsed > testMaxSleepTime+50*time.Millisecond {
				t.Errorf("very long timer triggered at wrong time: expected ~%v, got %v", testMaxSleepTime, elapsed)
			}
		case <-time.After(testMaxSleepTime + 100*time.Millisecond):
			t.Error("very long timer did not trigger within expected time")
		}
	})
}

func TestTimerChannelBehavior(t *testing.T) {
	// Set a small MaxSleepTime for testing
	setMaxSleepTimeForTest(t, testMaxSleepTime)

	t.Run("channel closed only once", func(t *testing.T) {
		start := clock.Now()
		target := start.Add(10 * time.Millisecond)
		timer := NewTimer(clock.Now, target)
		<-timer.C
		<-timer.C
		<-timer.C
		select {
		case <-timer.C:
		default:
			t.Error("timer channel should remain closed after trigger")
		}
	})

	t.Run("stopped timer channel not closed", func(t *testing.T) {
		start := clock.Now()
		target := start.Add(100 * time.Millisecond)
		timer := NewTimer(clock.Now, target)
		timer.Stop()
		time.Sleep(20 * time.Millisecond)
		select {
		case <-timer.C:
			t.Error("stopped timer channel should not be closed")
		default:
		}
	})
}
