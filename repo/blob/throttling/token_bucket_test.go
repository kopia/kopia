package throttling

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTokenBucket(t *testing.T) {
	b := newTokenBucket("test-bucket", 1000, 1000, time.Second)
	ctx := context.Background()

	currentTime := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)

	verifyTakeTimeElapsed := func(take float64, wantSleep time.Duration) {
		t0 := currentTime

		b.Take(ctx, take)

		diff := currentTime.Sub(t0)

		require.Equal(t, wantSleep, diff)
	}

	advanceTime := func(dur time.Duration) {
		currentTime = currentTime.Add(dur)
	}

	b.now = func() time.Time {
		return currentTime
	}
	b.sleep = func(ctx context.Context, d time.Duration) {
		currentTime = currentTime.Add(d)
	}

	verifyTakeTimeElapsed(0, 0)
	require.Equal(t, 1000.0, b.numTokens)

	// we did not sleep and grabbed all tokens.
	verifyTakeTimeElapsed(1000, 0)
	require.Equal(t, 0.0, b.numTokens)

	// token bucket is empty, consuming 500 will require waiting 0.5 seconds
	verifyTakeTimeElapsed(500, 500*time.Millisecond)
	require.Equal(t, -500.0, b.numTokens)

	// grabbing zero will reset tokens to zero based on passage of time.
	verifyTakeTimeElapsed(0, 0)
	require.Equal(t, 0.0, b.numTokens)

	advanceTime(1 * time.Second)
	verifyTakeTimeElapsed(0, 0)
	require.Equal(t, 1000.0, b.numTokens)

	// token bucket is empty at point, wait a long time to fully replenish.
	advanceTime(5 * time.Second)
	verifyTakeTimeElapsed(0, 0)

	require.Equal(t, 1000.0, b.numTokens)

	// now we can grab all tokens without sleeping
	verifyTakeTimeElapsed(300, 0)
	verifyTakeTimeElapsed(700, 0)
	verifyTakeTimeElapsed(1000, time.Second)
	verifyTakeTimeElapsed(100, 100*time.Millisecond)

	advanceTime(5 * time.Second)

	verifyTakeTimeElapsed(1000, 0)
	b.Return(ctx, 2000)
	verifyTakeTimeElapsed(1000, 0)
	b.Return(ctx, 1000)
	verifyTakeTimeElapsed(1000, 0)
}
