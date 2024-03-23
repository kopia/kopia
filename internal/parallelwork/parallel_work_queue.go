// Package parallelwork implements parallel work queue with fixed number of workers that concurrently process and add work items to the queue.
package parallelwork

import (
	"container/list"
	"context"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/clock"
)

// Queue represents a work queue with multiple parallel workers.
type Queue struct {
	monitor *sync.Cond

	queueItems        *list.List
	enqueuedWork      int64
	activeWorkerCount int64
	completedWork     int64

	nextReportTime time.Time

	ProgressCallback func(ctx context.Context, enqueued, active, completed int64)
}

// CallbackFunc is a callback function.
type CallbackFunc func() error

// EnqueueFront adds the work to the front of the queue.
func (v *Queue) EnqueueFront(ctx context.Context, callback CallbackFunc) {
	v.enqueue(ctx, true, callback)
}

// EnqueueBack adds the work to the back of the queue.
func (v *Queue) EnqueueBack(ctx context.Context, callback CallbackFunc) {
	v.enqueue(ctx, false, callback)
}

func (v *Queue) enqueue(ctx context.Context, front bool, callback CallbackFunc) {
	v.monitor.L.Lock()
	defer v.monitor.L.Unlock()

	v.enqueuedWork++

	// add to the queue and signal one reader
	if front {
		v.queueItems.PushFront(callback)
	} else {
		v.queueItems.PushBack(callback)
	}

	v.maybeReportProgress(ctx)
	v.monitor.Signal()
}

// Process starts N workers, which will be processing elements in the queue until the queue
// is empty and all workers are idle or until any of the workers returns an error.
func (v *Queue) Process(ctx context.Context, workers int) error {
	defer v.reportProgress(ctx)

	eg, ctx := errgroup.WithContext(ctx)

	for range workers {
		eg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					// context canceled - some other worker returned an error.
					return ctx.Err()

				default:
					callback := v.dequeue(ctx)
					if callback == nil {
						// no more work, shut down.
						return nil
					}

					err := callback()

					v.completed(ctx)

					if err != nil {
						return err
					}
				}
			}
		})
	}

	//nolint:wrapcheck
	return eg.Wait()
}

func (v *Queue) dequeue(ctx context.Context) CallbackFunc {
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
	v.maybeReportProgress(ctx)

	front := v.queueItems.Front()
	v.queueItems.Remove(front)

	return front.Value.(CallbackFunc) //nolint:forcetypeassert
}

func (v *Queue) completed(ctx context.Context) {
	v.monitor.L.Lock()
	defer v.monitor.L.Unlock()

	v.activeWorkerCount--
	v.completedWork++
	v.maybeReportProgress(ctx)

	v.monitor.Broadcast()
}

func (v *Queue) reportProgress(ctx context.Context) {
	cb := v.ProgressCallback
	if cb != nil {
		cb(ctx, v.enqueuedWork, v.activeWorkerCount, v.completedWork)
	}
}

func (v *Queue) maybeReportProgress(ctx context.Context) {
	if clock.Now().Before(v.nextReportTime) {
		return
	}

	v.nextReportTime = clock.Now().Add(1 * time.Second)

	v.reportProgress(ctx)
}

// OnNthCompletion invokes the provided callback once the returned callback function has been invoked exactly n times.
func OnNthCompletion(n int, callback CallbackFunc) CallbackFunc {
	var mu sync.Mutex

	return func() error {
		mu.Lock()
		n--
		call := n == 0
		mu.Unlock()

		if call {
			return callback()
		}

		return nil
	}
}

// NewQueue returns new parallel work queue.
func NewQueue() *Queue {
	return &Queue{
		queueItems: list.New(),
		monitor:    sync.NewCond(&sync.Mutex{}),
	}
}
