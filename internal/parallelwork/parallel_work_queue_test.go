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
		require.True(t, update.enqueued >= 0)
		require.True(t, update.active >= 0)
		require.True(t, update.completed >= 0)
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
		for i := 0; i < n-1; i++ {
			err := onNthCompletion()
			require.NoError(t, err)
			require.Equal(t, callbackInvoked, 0)
		}

		// on n-th invocation
		err := onNthCompletion()
		require.Error(t, err)
		require.ErrorIs(t, err, errCalled)
		require.Equal(t, callbackInvoked, 1)

		// call once again (after n-th invocation)
		err = onNthCompletion()
		require.NoError(t, err)
		require.Equal(t, callbackInvoked, 1)
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

		for i := 0; i < n+1; i++ {
			go func() {
				results <- onNthCompletion()
				wg.Done()
			}()
		}

		wg.Wait()
		close(results)

		// callback must be called exactly 1 time
		require.Equal(t, callbackInvoked.Load(), int32(1))

		var cnt int
		for result := range results {
			cnt++
			switch cnt {
			// n-th invocation must run and return an expected error
			case n:
				require.Error(t, result)
				require.ErrorIs(t, result, errCalled)
			// other invocations must not run and return any error
			default:
				require.NoError(t, result)
			}
		}
	})
}
