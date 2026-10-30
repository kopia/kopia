package clock_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/timetrack"
)

func TestSleepInterruptibly_ContextCanceled(t *testing.T) {
	t0 := timetrack.StartTimer()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	require.False(t, clock.SleepInterruptibly(ctx, 3*time.Second))

	dt := t0.Elapsed()

	require.Greater(t, dt, 90*time.Millisecond)
	require.Less(t, dt, time.Second)
}

func TestSleepInterruptibly_ContextNotCanceled(t *testing.T) {
	t0 := timetrack.StartTimer()

	require.True(t, clock.SleepInterruptibly(context.Background(), 100*time.Millisecond))

	dt := t0.Elapsed()

	require.Greater(t, dt, 90*time.Millisecond)
	require.Less(t, dt, time.Second)
}
