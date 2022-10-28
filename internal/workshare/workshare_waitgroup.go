// Package workshare implements work sharing worker pool.
//
// It is commonly used to traverse tree-like structures:
//
//	type processWorkRequest struct {
//	  node *someNode
//	  err error
//	}
//
//	func processWork(p *workshare.Pool, req interface{}) {
//	  req := req.(*processWorkRequest)
//	  req.err = visitNode(p, req.node)
//	}
//
//	func visitNode(p *workshare.Pool, n *someNode) error {
//	  var wg workshare.AsyncGroup
//	  defer wg.Close()
//
//	  for _, child := range n.children {
//	    if wg.CanShareWork(p) {
//	      // run asynchronously, collect result using wg.Wait() below.
//	      RunAsync(p, processWork, processWorkRequest{child})
//	    } else {
//		      if err := visitNode(p, n); err != nil {
//	         return err
//	       }
//	    }
//	  }
//
//	  // wait for results from all shared work and handle them.
//	  for _, req := range wg.Wait() {
//	    if err := req.(*processWorkRequest).err; err != nil {
//	      return err
//	    }
//	  }
//
//	  return nil
//	}
//
// wp = workshare.NewPool(10)
// defer wp.Close()
// visitNode(wp, root)
package workshare

import (
	"sync"
)

// AsyncGroup launches and awaits asynchronous work through a WorkerPool.
// It provides API designed to minimize allocations while being reasonably easy to use.
// AsyncGroup is very lightweight and NOT safe for concurrent use, all interactions must
// be from the same goroutine.
type AsyncGroup[T any] struct {
	wg       *sync.WaitGroup
	requests []T
	waited   bool
	isClosed bool
}

// Wait waits for scheduled asynchronous work to complete and returns all asynchronously processed inputs.
func (g *AsyncGroup[T]) Wait() []T {
	if g.isClosed {
		panic("Wait() can't be called after Close()")
	}

	if g.waited {
		panic("Wait() can't be called more than once")
	}

	g.waited = true

	if g.wg == nil {
		return nil
	}

	g.wg.Wait()

	return g.requests
}

// Close ensures all asynchronous work has been awaited for.
func (g *AsyncGroup[T]) Close() {
	if g.isClosed {
		return
	}

	if !g.waited {
		g.Wait() // ensure we wait
	}

	g.isClosed = true
}

// RunAsync starts the asynchronous work to process the provided request,
// the user must call Wait() after all RunAsync() have been scheduled.
func (g *AsyncGroup[T]) RunAsync(w *Pool[T], process ProcessFunc[T], request T) {
	if g.wg == nil {
		g.wg = &sync.WaitGroup{}
	}

	g.wg.Add(1)

	g.requests = append(g.requests, request)

	select {
	case w.work <- workItem[T]{
		process: process,
		request: request,
		wg:      g.wg,
	}:

	case <-w.closed:
		panic("invalid usage - RunAsync() after workshare.AsyncGroup has been closed")
	}
}

// CanShareWork determines if the provided worker pool has capacity to share work.
// If the function returns true, the use MUST call RunAsync() exactly once. This pattern avoids
// allocations required to create asynchronous input if the worker pool is full.
func (g *AsyncGroup[T]) CanShareWork(w *Pool[T]) bool {
	// check pool is not closed
	select {
	case <-w.closed:
		panic("invalid usage - CanShareWork() after workshare.AsyncGroup has been closed")

	default:
		select {
		case w.semaphore <- struct{}{}:
			// we successfully added token to the channel, because we have exactly the same number
			// of workers as the capacity of the channel, one worker will wake up to process
			// item from w.work, which will be added by RunAsync().
			return true

		default:
			// all workers are busy.
			return false
		}
	}
}
