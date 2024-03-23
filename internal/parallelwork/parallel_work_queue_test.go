package parallelwork_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/parallelwork"
)

func TestEnqueueFrontAndProcess(t *testing.T) {
	queue := parallelwork.NewQueue()

	results := make(chan int, 3)

	// Enqueue work items to the front of the queue
	queue.EnqueueFront(context.Background(), func() error {
		results <- 3
		return nil
	})
	queue.EnqueueFront(context.Background(), func() error {
		results <- 2
		return nil
	})
	queue.EnqueueFront(context.Background(), func() error {
		results <- 1
		return nil
	})

	err := queue.Process(context.Background(), 2) // Use two workers
	require.NoError(t, err)

	close(results)

	var sum int
	for res := range results {
		sum += res
	}

	require.Equal(t, 6, sum)
}

func TestEnqueueBackAndProcess(t *testing.T) {
	queue := parallelwork.NewQueue()

	results := make(chan int, 3)

	// Enqueue work items to the back of the queue
	queue.EnqueueBack(context.Background(), func() error {
		results <- 1
		return nil
	})
	queue.EnqueueBack(context.Background(), func() error {
		results <- 2
		return nil
	})
	queue.EnqueueBack(context.Background(), func() error {
		results <- 3
		return nil
	})

	err := queue.Process(context.Background(), 2) // Use two workers
	require.NoError(t, err)

	close(results)

	var sum int
	for res := range results {
		sum += res
	}

	require.Equal(t, 6, sum)
}

func TestProcessWithError(t *testing.T) {
	queue := parallelwork.NewQueue()

	testError := errors.New("test error") //nolint:goerr113

	// Enqueue work items, one of them returns an error
	queue.EnqueueBack(context.Background(), func() error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	queue.EnqueueBack(context.Background(), func() error {
		return testError
	})
	queue.EnqueueBack(context.Background(), func() error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	err := queue.Process(context.Background(), 2) // Use two workers
	require.Equal(t, testError, err)
}

func TestWaitForActiveWorkers(t *testing.T) {
	queue := parallelwork.NewQueue()

	results := make(chan int, 3)

	queue.EnqueueBack(context.Background(), func() error {
		time.Sleep(100 * time.Millisecond)
		queue.EnqueueBack(context.Background(), func() error {
			results <- 2
			return nil
		})
		results <- 1
		return nil
	})

	err := queue.Process(context.Background(), 1) // Use only one worker
	require.NoError(t, err)

	close(results)

	var sum int
	for res := range results {
		sum += res
	}

	require.Equal(t, 3, sum)
}

func TestProgressCallback(t *testing.T) {
	queue := parallelwork.NewQueue()

	progressUpdates := make(chan struct {
		enqueued, active, completed int64
	}, 3)

	queue.ProgressCallback = func(ctx context.Context, enqueued, active, completed int64) {
		progressUpdates <- struct {
			enqueued, active, completed int64
		}{enqueued, active, completed}
	}

	queue.EnqueueBack(context.Background(), func() error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	queue.EnqueueBack(context.Background(), func() error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	err := queue.Process(context.Background(), 2) // Use two workers
	require.NoError(t, err)

	close(progressUpdates)

	for update := range progressUpdates {
		require.GreaterOrEqual(t, update.enqueued, int64(0))
		require.GreaterOrEqual(t, update.active, int64(0))
		require.GreaterOrEqual(t, update.completed, int64(0))
	}
}

func TestOnNthCompletion(t *testing.T) {
	t.Run("callback is only called on n-th invocation", func(t *testing.T) {
		var (
			n               = 5                    // expect invocation on 5th attempt
			errCalled       = errors.New("called") //nolint:goerr113
			callbackInvoked int
			callback        = func() error {
				callbackInvoked++
				return errCalled
			}
		)

		onNthCompletion := parallelwork.OnNthCompletion(n, callback)

		// before n-th invocation
		for range n - 1 {
			err := onNthCompletion()
			require.NoError(t, err)
			require.Equal(t, 0, callbackInvoked)
		}

		// on n-th invocation
		err := onNthCompletion()
		require.Error(t, err)
		require.ErrorIs(t, err, errCalled)
		require.Equal(t, 1, callbackInvoked)

		// call once again (after n-th invocation)
		err = onNthCompletion()
		require.NoError(t, err)
		require.Equal(t, 1, callbackInvoked)
	})

	t.Run("concurrency-safe", func(t *testing.T) {
		var (
			n               = 5                     // expect invocation on 5th attempt
			results         = make(chan error, n+1) // we will have n+1, i.e. 6 attempts in total
			errCalled       = errors.New("called")  //nolint:goerr113
			callbackInvoked atomic.Int32
			wg              sync.WaitGroup
			callback        = func() error {
				callbackInvoked.Add(1)
				return errCalled
			}
		)

		onNthCompletion := parallelwork.OnNthCompletion(n, callback)

		wg.Add(n + 1)

		for range n + 1 {
			go func() {
				results <- onNthCompletion()
				wg.Done()
			}()
		}

		wg.Wait()
		close(results)

		// callback must be called exactly 1 time
		require.Equal(t, int32(1), callbackInvoked.Load())

		var (
			errCalledCount int
			noErrorCount   int
		)

		for result := range results {
			if result == nil {
				noErrorCount++
				continue
			}

			errCalledCount++

			require.ErrorIs(t, result, errCalled)
		}

		require.Equal(t, 1, errCalledCount)
		require.Equal(t, n, noErrorCount)
	})
}
