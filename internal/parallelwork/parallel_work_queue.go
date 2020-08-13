// Package parallelwork implements pallel work queue with fixed number of workers that concurrently process and add work items to the queue.
package parallelwork

import (
	"container/list"
	"context"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// Queue represents a work queue with multiple parallel workers.
type Queue struct {
	monitor *sync.Cond

	queueItems        *list.List
	enqueuedWork      int64
	activeWorkerCount int64
	completedWork     int64

	nextReportTime time.Time

	ProgressCallback func(enqueued, active, completed int64)
}

// CallbackFunc is a callback function.
type CallbackFunc func() error

// EnqueueFront adds the work to the front of the queue.
func (v *Queue) EnqueueFront(callback CallbackFunc) {
	v.enqueue(true, callback)
}

// EnqueueBack adds the work to the back of the queue.
func (v *Queue) EnqueueBack(callback CallbackFunc) {
	v.enqueue(false, callback)
}

func (v *Queue) enqueue(front bool, callback CallbackFunc) {
	v.monitor.L.Lock()
	defer v.monitor.L.Unlock()

	v.enqueuedWork++

	// add to the queue and signal one reader
	if front {
		v.queueItems.PushFront(callback)
	} else {
		v.queueItems.PushBack(callback)
	}

	v.maybeReportProgress()
	v.monitor.Signal()
}

// Process starts N workers, which will be processing elements in the queue until the queue
// is empty and all workers are idle or until any of the workers returns an error.
func (v *Queue) Process(workers int) error {
	eg, ctx := errgroup.WithContext(context.Background())

	for i := 0; i < workers; i++ {
		eg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					// context canceled - some other worker returned an error.
					return ctx.Err()

				default:
					callback := v.dequeue()
					if callback == nil {
						// no more work, shut down.
						return nil
					}

					err := callback()
					v.completed()
					if err != nil {
						return err
					}
				}
			}
		})
	}

	return eg.Wait()
}

func (v *Queue) dequeue() CallbackFunc {
	v.monitor.L.Lock()
	defer v.monitor.L.Unlock()

	for v.queueItems.Len() == 0 && v.activeWorkerCount > 0 {
		// no items in queue, but some workers are active, they may add more.
		v.monitor.Wait()
	}

	// no items in queue, no workers are active, no more work.
	if v.queueItems.Len() == 0 {
		return nil
	}

	v.activeWorkerCount++
	v.maybeReportProgress()

	front := v.queueItems.Front()
	v.queueItems.Remove(front)

	return front.Value.(CallbackFunc)
}

func (v *Queue) completed() {
	v.monitor.L.Lock()
	defer v.monitor.L.Unlock()

	v.activeWorkerCount--
	v.completedWork++
	v.maybeReportProgress()

	v.monitor.Broadcast()
}

func (v *Queue) maybeReportProgress() {
	cb := v.ProgressCallback
	if cb == nil {
		return
	}

	if time.Now().Before(v.nextReportTime) {
		return
	}

	v.nextReportTime = time.Now().Add(1 * time.Second)

	cb(v.enqueuedWork, v.activeWorkerCount, v.completedWork)
}

// NewQueue returns new parallel work queue.
func NewQueue() *Queue {
	return &Queue{
		queueItems: list.New(),
		monitor:    sync.NewCond(&sync.Mutex{}),
	}
}
