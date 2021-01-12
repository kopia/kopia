package content

import (
	"container/heap"

	"github.com/pkg/errors"
)

const iterateParallelism = 16

// mergedIndex is an implementation of Index that transparently merges returns from underlying Indexes.
type mergedIndex []packIndex

func (m mergedIndex) ApproximateCount() int {
	c := 0

	for _, ndx := range m {
		c += ndx.ApproximateCount()
	}

	return c
}

// Close closes all underlying indexes.
func (m mergedIndex) Close() error {
	for _, ndx := range m {
		if err := ndx.Close(); err != nil {
			return errors.Wrap(err, "error closing index shard")
		}
	}

	return nil
}

// GetInfo returns information about a single content. If a content is not found, returns (nil,nil).
func (m mergedIndex) GetInfo(id ID) (*Info, error) {
	var best *Info

	for _, ndx := range m {
		i, err := ndx.GetInfo(id)
		if err != nil {
			return nil, errors.Wrapf(err, "error getting id %v from index shard", id)
		}

		if i != nil {
			if best == nil || i.TimestampSeconds > best.TimestampSeconds || (i.TimestampSeconds == best.TimestampSeconds && !i.Deleted) {
				best = i
			}
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
	if a, b := h[i].it.ID, h[j].it.ID; a != b {
		return a < b
	}

	if a, b := h[i].it.TimestampSeconds, h[j].it.TimestampSeconds; a != b {
		return a < b
	}

	return !h[i].it.Deleted
}

func (h nextInfoHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *nextInfoHeap) Push(x interface{}) {
	*h = append(*h, x.(*nextInfo))
}

func (h *nextInfoHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]

	return x
}

func iterateChan(r IDRange, ndx packIndex, done chan bool) <-chan Info {
	ch := make(chan Info, iterateParallelism)

	go func() {
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
func (m mergedIndex) Iterate(r IDRange, cb func(i Info) error) error {
	var minHeap nextInfoHeap

	done := make(chan bool)

	defer close(done)

	for _, ndx := range m {
		ch := iterateChan(r, ndx, done)

		it, ok := <-ch
		if ok {
			heap.Push(&minHeap, &nextInfo{it, ch})
		}
	}

	var pendingItem Info

	for len(minHeap) > 0 {
		min := heap.Pop(&minHeap).(*nextInfo)
		if pendingItem.ID != min.it.ID {
			if pendingItem.ID != "" {
				if err := cb(pendingItem); err != nil {
					return err
				}
			}

			pendingItem = min.it
		} else if min.it.TimestampSeconds > pendingItem.TimestampSeconds {
			pendingItem = min.it
		}

		it, ok := <-min.ch
		if ok {
			heap.Push(&minHeap, &nextInfo{it, min.ch})
		}
	}

	if pendingItem.ID != "" {
		return cb(pendingItem)
	}

	return nil
}

var _ packIndex = (*mergedIndex)(nil)
