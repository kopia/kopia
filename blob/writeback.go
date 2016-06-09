// Wrapper which implements asynchronous (write-back) PutBlock and DeleteBlock operation
// useful for slower backends (cloud).

package blob

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type writeBackStorage struct {
	Storage

	channel       chan writeBackRequest
	deferredError atomic.Value
	workerCount   int
}

type writeBackRequest struct {
	action        func() error
	workerPaused  *sync.WaitGroup
	workerRelease *sync.WaitGroup
	debugInfo     string
}

func (wb *writeBackStorage) PutBlock(blockID string, data ReaderWithLength, options PutOptions) error {
	err := wb.getDeferredError()
	if err != nil {
		data.Close()
		return err
	}

	wb.channel <- writeBackRequest{
		action: func() error {
			return wb.Storage.PutBlock(blockID, data, options)
		},
		debugInfo: fmt.Sprintf("Put(%s)", blockID),
	}
	return nil
}

func (wb *writeBackStorage) getDeferredError() error {
	deferredError := wb.deferredError.Load()
	if deferredError != nil {
		return deferredError.(error)
	}

	return nil
}

func (wb *writeBackStorage) DeleteBlock(blockID string) error {
	wb.channel <- writeBackRequest{
		action: func() error {
			return wb.Storage.DeleteBlock(blockID)
		},
		debugInfo: fmt.Sprintf("Delete(%s)", blockID),
	}
	return nil
}

func (wb *writeBackStorage) Flush() error {
	rwg := sync.WaitGroup{}
	rwg.Add(1)

	// Create a wait group that all workers will join.
	wg := sync.WaitGroup{}
	wg.Add(wb.workerCount)

	// Send a request to all workers that causes them to report to the waitgroup.
	for n := 0; n < wb.workerCount; n++ {
		wb.channel <- writeBackRequest{
			workerPaused:  &wg,
			workerRelease: &rwg,
		}
	}

	// Wait until all workers join the wait group.
	wg.Wait()

	// Now release them all.
	rwg.Done()

	return wb.Storage.Flush()
}

func (wb *writeBackStorage) processRequest(req writeBackRequest) {
	if req.workerPaused != nil {
		req.workerPaused.Done()
		req.workerRelease.Wait()
		return
	}
	if wb.getDeferredError() != nil {
		return
	}

	err := req.action()
	if err != nil {
		wb.deferredError.Store(err)
	}
}

// NewWriteBackWrapper returns a Storage wrapper that processes writes asynchronously using the specified
// number of worker goroutines. This wrapper is best used with Repositories that exhibit high latency.
func NewWriteBackWrapper(wrapped Storage, workerCount int) Storage {
	ch := make(chan writeBackRequest, workerCount)
	result := &writeBackStorage{
		Storage:     wrapped,
		channel:     ch,
		workerCount: workerCount,
	}

	for i := 0; i < workerCount; i++ {
		go func(workerId int) {
			for {
				req, ok := <-ch
				if !ok {
					break
				}

				result.processRequest(req)
			}
		}(i)
	}

	return result
}
