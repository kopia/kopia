package throttling

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/timetrack"
)

func TestThrottler(t *testing.T) {
	limits := Limits{
		ReadsPerSecond:         10,
		WritesPerSecond:        10,
		ListsPerSecond:         10,
		UploadBytesPerSecond:   1000,
		DownloadBytesPerSecond: 1000,
	}

	const window = time.Second

	ctx := context.Background()
	th, err := NewThrottler(limits, window, 0.0 /* start empty */)
	require.NoError(t, err)
	require.Equal(t, limits, th.Limits())

	testRateLimiting(t, "DownloadBytesPerSecond", limits.DownloadBytesPerSecond, func(total *int64) {
		numBytes := rand.Int63n(1500)
		excess := rand.Int63n(10)
		th.BeforeDownload(ctx, numBytes+excess)
		th.ReturnUnusedDownloadBytes(ctx, excess)
		atomic.AddInt64(total, numBytes)
	})

	th, err = NewThrottler(limits, window, 0.0 /* start empty */)
	require.NoError(t, err)
	testRateLimiting(t, "UploadBytesPerSecond", limits.UploadBytesPerSecond, func(total *int64) {
		numBytes := rand.Int63n(1500)
		th.BeforeUpload(ctx, numBytes)
		atomic.AddInt64(total, numBytes)
	})

	th, err = NewThrottler(limits, window, 0.0 /* start empty */)
	require.NoError(t, err)
	testRateLimiting(t, "ReadsPerSecond", limits.ReadsPerSecond, func(total *int64) {
		th.BeforeOperation(ctx, "GetBlob")
		atomic.AddInt64(total, 1)
	})

	th, err = NewThrottler(limits, window, 0.0 /* start empty */)
	require.NoError(t, err)
	testRateLimiting(t, "WritesPerSecond", limits.WritesPerSecond, func(total *int64) {
		th.BeforeOperation(ctx, "PutBlob")
		atomic.AddInt64(total, 1)
	})

	th, err = NewThrottler(limits, window, 0.0 /* start empty */)
	require.NoError(t, err)
	testRateLimiting(t, "ListsPerSecond", limits.ListsPerSecond, func(total *int64) {
		th.BeforeOperation(ctx, "ListBlobs")
		atomic.AddInt64(total, 1)
	})
}

func TestThrottlerLargeWindow(t *testing.T) {
	limits := Limits{
		ReadsPerSecond:         10,
		WritesPerSecond:        10,
		ListsPerSecond:         10,
		UploadBytesPerSecond:   1000,
		DownloadBytesPerSecond: 1000,
	}

	ctx := context.Background()
	th, err := NewThrottler(limits, time.Minute, 1.0 /* start full */)
	require.NoError(t, err)

	// make sure we can consume 60x worth the quota without
	timer := timetrack.StartTimer()

	th.BeforeDownload(ctx, 60000)
	require.Less(t, timer.Elapsed(), 500*time.Millisecond)

	// subsequent call will block
	timer = timetrack.StartTimer()

	th.BeforeDownload(ctx, 1000)
	require.Greater(t, timer.Elapsed(), 900*time.Millisecond)
}

//nolint:thelper
func testRateLimiting(t *testing.T, name string, wantRate float64, worker func(total *int64)) {
	t.Run(name, func(t *testing.T) {
		t.Parallel()

		const (
			testDuration = 3 * time.Second
			numWorkers   = 3
		)

		deadline := clock.Now().Add(testDuration)
		total := new(int64)

		timer := timetrack.StartTimer()

		var wg sync.WaitGroup

		for range numWorkers {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for clock.Now().Before(deadline) {
					worker(total)
				}
			}()
		}

		wg.Wait()

		numSeconds := timer.Elapsed().Seconds()
		actualRate := float64(*total) / numSeconds

		// make sure the rate is less than target with some tiny margin of error
		require.Less(t, actualRate, wantRate*1.15)
		require.Greater(t, actualRate, wantRate*0.85)
	})
}
