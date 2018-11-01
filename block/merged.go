package block

import (
	"container/heap"
	"errors"
)

// mergedIndex is an implementation of Index that transparently merges retuns from underlying Indexes.
type mergedIndex []packIndex

// Close closes all underlying indexes.
func (m mergedIndex) Close() error {
	for _, ndx := range m {
		if err := ndx.Close(); err != nil {
			return err
		}
	}

	return nil
}

// GetInfo returns information about a single block. If a block is not found, returns (nil,nil)
func (m mergedIndex) GetInfo(contentID string) (*Info, error) {
	var best *Info
	for _, ndx := range m {
		i, err := ndx.GetInfo(contentID)
		if err != nil {
			return nil, err
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
	if a, b := h[i].it.BlockID, h[j].it.BlockID; a != b {
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

func iterateChan(prefix string, ndx packIndex, done chan bool) <-chan Info {
	ch := make(chan Info)
	go func() {
		defer close(ch)

		_ = ndx.Iterate(prefix, func(i Info) error {
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

// Iterate invokes the provided callback for all unique block IDs in the underlying sources until either
// all blocks have been visited or until an error is returned by the callback.
func (m mergedIndex) Iterate(prefix string, cb func(i Info) error) error {
	var minHeap nextInfoHeap
	done := make(chan bool)
	defer close(done)

	for _, ndx := range m {
		ch := iterateChan(prefix, ndx, done)
		it, ok := <-ch
		if ok {
			heap.Push(&minHeap, &nextInfo{it, ch})
		}
	}

	var pendingItem Info

	for len(minHeap) > 0 {
		min := heap.Pop(&minHeap).(*nextInfo)
		if pendingItem.BlockID != min.it.BlockID {
			if pendingItem.BlockID != "" {
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

	if pendingItem.BlockID != "" {
		return cb(pendingItem)
	}

	return nil
}

var _ packIndex = (*mergedIndex)(nil)
