// Package workshare implements work sharing worker pool.
package workshare

import (
	"sync"
	"sync/atomic"
)

// ProcessFunc processes the provided request, which is typically a pointer to a structure.
// The result will typically be written within the request structure. To avoid allocations,
// the function should not be a local closure but a named function (either global or a method).
type ProcessFunc[T any] func(c *Pool[T], request T)

type workItem[T any] struct {
	process ProcessFunc[T]  // function to call
	request T               // parameter to the function
	wg      *sync.WaitGroup // signal the provided WaitGroup after work is done
}

// Pool manages a pool of generic workers that can process workItem.
type Pool[T any] struct {
	// +checkatomic
	activeWorkers int32

	semaphore chan struct{}

	work   chan workItem[T]
	closed chan struct{}

	wg sync.WaitGroup
}

// ActiveWorkers returns the number of active workers.
func (w *Pool[T]) ActiveWorkers() int {
	return int(atomic.LoadInt32(&w.activeWorkers))
}

// NewPool creates a worker pool that launches a given number of goroutines that can invoke shared work.
func NewPool[T any](numWorkers int) *Pool[T] {
	if numWorkers < 0 {
		numWorkers = 0
	}

	w := &Pool[T]{
		// channel must be unbuffered so that it has exactly as many slots as there are goroutines capable of reading from it
		// this way by pushing to the channel we can be sure that a pre-spun goroutine will pick it up soon.
		work:      make(chan workItem[T]),
		closed:    make(chan struct{}),
		semaphore: make(chan struct{}, numWorkers),
	}

	for i := 0; i < numWorkers; i++ {
		w.wg.Add(1)

		go func() {
			defer w.wg.Done()

			for {
				select {
				case it := <-w.work:
					atomic.AddInt32(&w.activeWorkers, 1)
					it.process(w, it.request)
					atomic.AddInt32(&w.activeWorkers, -1)
					<-w.semaphore
					it.wg.Done()

				case <-w.closed:
					return
				}
			}
		}()
	}

	return w
}

// Close closes the worker pool.
func (w *Pool[T]) Close() {
	close(w.closed)
	w.wg.Wait()
}
