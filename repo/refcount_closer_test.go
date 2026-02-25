package repo

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRefCountedCloserRace tests that the race condition between Close() and addRef() is properly handled
func TestRefCountedCloserRace(t *testing.T) {
	t.Parallel()

	// Run the test multiple times to increase chance of catching races
	for i := 0; i < 100; i++ {
		t.Run("iteration", func(t *testing.T) {
			closed := false
			rcc := newRefCountedCloser(func(ctx context.Context) error {
				closed = true
				return nil
			})

			var wg sync.WaitGroup
			wg.Add(2)

			addRefSucceeded := false

			// Goroutine 1: Tries to add a ref
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						// Expected panic when trying to addRef after close
						require.Equal(t, "already closed", r)
					}
				}()
				// Small delay to increase likelihood of race
				time.Sleep(time.Microsecond)
				rcc.addRef()
				addRefSucceeded = true
			}()

			// Goroutine 2: Closes the closer
			go func() {
				defer wg.Done()
				_ = rcc.Close(context.Background())
			}()

			wg.Wait()

			// If addRef succeeded, we need to close again to trigger the close function
			if addRefSucceeded {
				_ = rcc.Close(context.Background())
			}

			require.True(t, closed, "closer should be closed")
		})
	}
}

// TestRefCountedCloserNormal tests normal operation without races
func TestRefCountedCloserNormal(t *testing.T) {
	t.Parallel()

	closeCount := 0
	rcc := newRefCountedCloser(func(ctx context.Context) error {
		closeCount++
		return nil
	})

	// Add some refs
	rcc.addRef()
	rcc.addRef()

	// Close multiple times, but close func should only be called once
	require.NoError(t, rcc.Close(context.Background()))
	require.NoError(t, rcc.Close(context.Background()))
	require.NoError(t, rcc.Close(context.Background()))
	require.NoError(t, rcc.Close(context.Background()))

	require.Equal(t, 1, closeCount, "close function should be called exactly once")

	// Trying to addRef after close should panic
	require.Panics(t, func() {
		rcc.addRef()
	})
}
