// Wrapper which implements asynchronous (write-back) PutBlock and DeleteBlock operation
// useful for slower backends (cloud).

package storage

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

type writeBackRepository struct {
	Repository

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

func (wb *writeBackRepository) PutBlock(blockID BlockID, data io.ReadCloser, options PutOptions) error {
	err := wb.getDeferredError()
	if err != nil {
		data.Close()
		return err
	}

	wb.channel <- writeBackRequest{
		action: func() error {
			return wb.Repository.PutBlock(blockID, data, options)
		},
		debugInfo: fmt.Sprintf("Put(%s)", blockID),
	}
	return nil
}

func (wb *writeBackRepository) getDeferredError() error {
	deferredError := wb.deferredError.Load()
	if deferredError != nil {
		return deferredError.(error)
	}

	return nil
}

func (wb *writeBackRepository) DeleteBlock(blockID BlockID) error {
	wb.channel <- writeBackRequest{
		action: func() error {
			return wb.Repository.DeleteBlock(blockID)
		},
		debugInfo: fmt.Sprintf("Delete(%s)", blockID),
	}
	return nil
}

func (wb *writeBackRepository) Flush() error {
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

	return wb.Repository.Flush()
}

func (wb *writeBackRepository) processRequest(req writeBackRequest) {
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

// NewWriteBackWrapper returns a Repository wrapper that processes writes asynchronously using the specified
// number of worker goroutines. This wrapper is best used with Repositories that exhibit high latency.
func NewWriteBackWrapper(wrapped Repository, workerCount int) Repository {
	ch := make(chan writeBackRequest, workerCount)
	result := &writeBackRepository{
		Repository:  wrapped,
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
