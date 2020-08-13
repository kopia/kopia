package buf

import (
	"context"
	"runtime"
	"sync"
	"testing"
)

func TestPool(t *testing.T) {
	var wg sync.WaitGroup

	ctx := context.Background()

	// 20 buffers of 1 MB each
	a := NewPool(ctx, 1000000, "testing-pool")
	defer a.Close()

	a.AddSegments(20)

	var ms1, ms2 runtime.MemStats

	runtime.ReadMemStats(&ms1)

	// 30 gorouties, each allocating and releasing memory 1 M times
	for i := 0; i < 30; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 1000000; j++ {
				const allocSize = 100000

				b := a.Allocate(allocSize)

				if got, want := len(b.Data), allocSize; got != want {
					t.Errorf("unexpected len: %v, want %v", got, want)
				}

				if got, want := cap(b.Data), allocSize; got != want {
					t.Errorf("unexpected cap: %v, want %v", got, want)
				}

				if !b.IsPooled() {
					t.Errorf("unexpected !IsPooled()")
				}

				b.Release()
			}
		}()
	}

	wg.Wait()
	runtime.ReadMemStats(&ms2)

	// amount of memory should be O(kilobytes), not 1 MB because all buffers got preallocated
	if diff := ms2.TotalAlloc - ms1.TotalAlloc; diff > 1000000 {
		t.Errorf("too much memory was allocated: %v", diff)
	}
}

func TestNilPool(t *testing.T) {
	var a *Pool

	// allocate from nil pool
	b := a.Allocate(5)

	if got, want := len(b.Data), 5; got != want {
		t.Errorf("unexpected len: %v, want %v", got, want)
	}

	if got, want := cap(b.Data), 5; got != want {
		t.Errorf("unexpected cap: %v, want %v", got, want)
	}

	if b.IsPooled() {
		t.Errorf("unexpected IsPooled()")
	}

	b.Release()
}
