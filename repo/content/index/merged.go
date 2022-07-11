package index

import (
	"container/heap"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

// Merged is an implementation of Index that transparently merges returns from underlying Indexes.
type Merged []Index

// ApproximateCount implements Index interface.
func (m Merged) ApproximateCount() int {
	c := 0

	for _, ndx := range m {
		c += ndx.ApproximateCount()
	}

	return c
}

// Close closes all underlying indexes.
func (m Merged) Close() error {
	var err error

	for _, ndx := range m {
		err = multierr.Append(err, ndx.Close())
	}

	return errors.Wrap(err, "closing index shards")
}

func contentInfoGreaterThan(a, b Info) bool {
	if b == nil {
		// everyrhing is greater than nil
		return true
	}

	if a == nil {
		// nil is less than everything
		return false
	}

	if l, r := a.GetTimestampSeconds(), b.GetTimestampSeconds(); l != r {
		// different timestamps, higher one wins
		return l > r
	}

	if l, r := a.GetDeleted(), b.GetDeleted(); l != r {
		// non-deleted is greater than deleted.
		return !a.GetDeleted()
	}

	// both same time, both deleted, we must ensure we always resolve to the same pack blob.
	// since pack blobs are random and unique, simple lexicographic ordering will suffice.
	return a.GetPackBlobID() > b.GetPackBlobID()
}

// GetInfo returns information about a single content. If a content is not found, returns (nil,nil).
func (m Merged) GetInfo(id ID) (Info, error) {
	var best Info

	for _, ndx := range m {
		i, err := ndx.GetInfo(id)
		if err != nil {
			return nil, errors.Wrapf(err, "error getting id %v from index shard", id)
		}

		if contentInfoGreaterThan(i, best) {
			best = i
		}
	}

	return best, nil
}

type nextInfo struct {
	it Info
	ch <-chan Info
}

type nextInfoHeap []*nextInfo

func (h nextInfoHeap) Len() int { return len(h) }
func (h nextInfoHeap) Less(i, j int) bool {
	if a, b := h[i].it.GetContentID(), h[j].it.GetContentID(); a != b {
		return a.less(b)
	}

	return !contentInfoGreaterThan(h[i].it, h[j].it)
}

func (h nextInfoHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *nextInfoHeap) Push(x interface{}) {
	*h = append(*h, x.(*nextInfo)) // nolint:forcetypeassert
}

func (h *nextInfoHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]

	return x
}

func iterateChan(r IDRange, ndx Index, done chan bool, wg *sync.WaitGroup) <-chan Info {
	ch := make(chan Info, 1)

	go func() {
		defer wg.Done()
		defer close(ch)

		_ = ndx.Iterate(r, func(i Info) error {
			select {
			case <-done:
				return errors.New("end of iteration")
			case ch <- i:
				return nil
			}
		})
	}()

	return ch
}

// Iterate invokes the provided callback for all unique content IDs in the underlying sources until either
// all contents have been visited or until an error is returned by the callback.
func (m Merged) Iterate(r IDRange, cb func(i Info) error) error {
	var minHeap nextInfoHeap

	done := make(chan bool)

	wg := &sync.WaitGroup{}

	for _, ndx := range m {
		wg.Add(1)

		ch := iterateChan(r, ndx, done, wg)

		it, ok := <-ch
		if ok {
			heap.Push(&minHeap, &nextInfo{it, ch})
		}
	}

	// make sure all iterateChan() complete before we return, otherwise they may be trying to reference
	// index structures that have been closed.
	defer wg.Wait()
	defer close(done)

	var pendingItem Info

	for len(minHeap) > 0 {
		// nolint:forcetypeassert
		min := heap.Pop(&minHeap).(*nextInfo)
		if pendingItem == nil || pendingItem.GetContentID() != min.it.GetContentID() {
			if pendingItem != nil {
				if err := cb(pendingItem); err != nil {
					return err
				}
			}

			pendingItem = min.it
		} else if contentInfoGreaterThan(min.it, pendingItem) {
			pendingItem = min.it
		}

		it, ok := <-min.ch
		if ok {
			heap.Push(&minHeap, &nextInfo{it, min.ch})
		}
	}

	if pendingItem != nil {
		return cb(pendingItem)
	}

	return nil
}

var _ Index = (*Merged)(nil)
