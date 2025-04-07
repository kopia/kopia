package throttling

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestThrottlingSemaphore(t *testing.T) {
	s := newSemaphore()
	// default is unlimited
	s.Acquire()
	s.Release()

	require.Error(t, s.SetLimit(-1))

	for _, lim := range []int{3, 5, 7} {
		require.NoError(t, s.SetLimit(lim))

		var (
			wg             sync.WaitGroup
			mu             sync.Mutex
			concurrency    int
			maxConcurrency int
		)

		for range 10 {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for range 10 {
					s.Acquire()

					mu.Lock()
					concurrency++

					if concurrency > maxConcurrency {
						maxConcurrency = concurrency
					}

					mu.Unlock()

					time.Sleep(10 * time.Millisecond)

					mu.Lock()
					concurrency--
					mu.Unlock()

					s.Release()
				}
			}()
		}

		wg.Wait()

		// Equal() would probably work here due to Sleep(), but not risking a flake.
		require.LessOrEqual(t, maxConcurrency, lim)
		require.Positive(t, maxConcurrency)
	}
}
