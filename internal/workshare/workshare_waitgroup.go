// Package workshare implements work sharing worker pool.
package workshare

import (
	"sync"
)

// AsyncGroup launches and awaits asynchronous work through a WorkerPool.
// It provides API designed to minimize allocations while being reasonably easy to use.
type AsyncGroup struct {
	wg       *sync.WaitGroup
	requests []interface{}
}

// Wait waits for scheduled asynchronous work to complete and returns all asynchronously processed inputs.
func (g *AsyncGroup) Wait() []interface{} {
	if g.wg == nil {
		return nil
	}

	g.wg.Wait()

	return g.requests
}

// RunAsync starts the asynchronous work to process the provided input, the user must call Wait().
func (g *AsyncGroup) RunAsync(w *Pool, process ProcessFunc, input interface{}) {
	if g.wg == nil {
		g.wg = &sync.WaitGroup{}
	}

	g.wg.Add(1)

	g.requests = append(g.requests, input)

	w.work <- workItem{
		process: process,
		input:   input,
		wg:      g.wg,
	}
}

// CanShareWork determines if the provided worker pool has capacity to share work.
// If the function returns true, the use MUST call RunAsync() exactly once. This pattern avoids
// allocations required to create asynchronous input if the worker pool is full.
func (g *AsyncGroup) CanShareWork(w *Pool) bool {
	select {
	case w.semaphore <- struct{}{}:
		return true

	default:
		return false
	}
}
