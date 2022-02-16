// Package workshare implements work sharing worker pool.
package workshare

import (
	"sync"
	"sync/atomic"
)

// ProcessFunc processes the provided input, the result must be written within the input itself.
// To avoid allocations, the function should not be a local closure but a named function (either global or
// a method).
type ProcessFunc func(c *Pool, input interface{})

type workItem struct {
	process ProcessFunc     // function to call
	input   interface{}     // parameter to the function
	wg      *sync.WaitGroup // signal the provided WaitGroup after work is done
}

// Pool manages a pool of generic workers that can process workItem.
type Pool struct {
	activeWorkers int32

	semaphore chan struct{}

	work   chan workItem
	closed chan struct{}

	wg sync.WaitGroup
}

// ActiveWorkers returns the number of active workers.
func (w *Pool) ActiveWorkers() int {
	return int(atomic.LoadInt32(&w.activeWorkers))
}

// NewPool creates a worker pool that launches a given number of goroutines that can invoke shared work.
func NewPool(numWorkers int) *Pool {
	if numWorkers < 0 {
		numWorkers = 0
	}

	w := &Pool{
		work:      make(chan workItem), // channel must be unbuffered
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
					it.process(w, it.input)
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
func (w *Pool) Close() {
	close(w.closed)
}
