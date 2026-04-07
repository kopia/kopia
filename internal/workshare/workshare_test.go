package workshare_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/workshare"
)

type treeNode struct {
	value    int
	children []*treeNode
}

func buildTree(level int) *treeNode {
	n := &treeNode{
		value: 1,
	}

	if level <= 0 {
		return n
	}

	for range level {
		n.children = append(n.children, buildTree(level-1))
	}

	return n
}

type computeTreeSumRequest struct {
	input *treeNode

	result int
	err    error
}

func dispatchComputeTreeSumRequest(w *workshare.Pool[*computeTreeSumRequest], req *computeTreeSumRequest) {
	if w.ActiveWorkers() == 0 {
		panic("unexpected worker count")
	}

	res, err := computeTreeSum(w, req.input)
	if err != nil {
		req.err = err
		return
	}

	req.result = res
}

func computeTreeSum(workPool *workshare.Pool[*computeTreeSumRequest], n *treeNode) (int, error) {
	total := n.value

	var cs workshare.AsyncGroup[*computeTreeSumRequest]

	for _, child := range n.children {
		if cs.CanShareWork(workPool) {
			// run the request on another goroutine, the results will be available
			cs.RunAsync(workPool, dispatchComputeTreeSumRequest, &computeTreeSumRequest{
				input: child,
			})
		} else {
			chtot, err := computeTreeSum(workPool, child)
			if err != nil {
				return 0, err
			}

			total += chtot
		}
	}

	for _, twr := range cs.Wait() {
		if twr.err != nil {
			return 0, twr.err
		}

		total += twr.result
	}

	return total, nil
}

func TestComputeTreeSum10(t *testing.T) {
	testComputeTreeSum(t, 10)
}

func TestComputeTreeSum1(t *testing.T) {
	testComputeTreeSum(t, 1)
}

func TestComputeTreeSum0(t *testing.T) {
	testComputeTreeSum(t, 0)
}

func TestComputeTreeSumNegative(t *testing.T) {
	testComputeTreeSum(t, -1)
}

func TestDisallowed_DoubleWait(t *testing.T) {
	var ag workshare.AsyncGroup[int]

	ag.Wait()
	require.Panics(t, func() {
		ag.Wait()
	})
}

func TestDisallowed_WaitAfterClose(t *testing.T) {
	var ag workshare.AsyncGroup[int]

	ag.Close()
	require.Panics(t, func() {
		ag.Wait()
	})

	ag.Close() // no-op
}

func TestDisallowed_UseAfterPoolClose(t *testing.T) {
	w := workshare.NewPool[int](1)

	var ag workshare.AsyncGroup[int]

	w.Close()

	require.Panics(t, func() {
		ag.CanShareWork(w)
	})

	require.Panics(t, func() {
		ag.RunAsync(w, func(c *workshare.Pool[int], request int) {
			t.Fatal("should not be called")
		}, 33)
	})
}

//nolint:thelper
func testComputeTreeSum(t *testing.T, numWorkers int) {
	w := workshare.NewPool[*computeTreeSumRequest](numWorkers)
	defer w.Close()

	n := buildTree(6)

	sum, err := computeTreeSum(w, n)
	require.NoError(t, err)
	require.Equal(t, 1957, sum)
}

func TestWorkerRecoversPanic(t *testing.T) {
	type req struct {
		shouldPanic bool
		result      int
	}

	w := workshare.NewPool[*req](4)
	defer w.Close()

	var ag workshare.AsyncGroup[*req]

	// Schedule a panicking work item.
	if ag.CanShareWork(w) {
		ag.RunAsync(w, func(_ *workshare.Pool[*req], r *req) {
			if r.shouldPanic {
				panic("test panic in workshare worker")
			}
			r.result = 42
		}, &req{shouldPanic: true})
	}

	// Schedule a normal work item — must still complete.
	if ag.CanShareWork(w) {
		ag.RunAsync(w, func(_ *workshare.Pool[*req], r *req) {
			r.result = 99
		}, &req{})
	}

	results := ag.Wait()

	// Both work items completed (no deadlock).
	require.Len(t, results, 2)

	// The panicking item's result is zero (never set).
	require.Equal(t, 0, results[0].result)

	// The normal item completed successfully.
	require.Equal(t, 99, results[1].result)
}

func TestWorkerRecoversPanic_PoolRemainsOperational(t *testing.T) {
	type req struct {
		result int
	}

	w := workshare.NewPool[*req](2)
	defer w.Close()

	// First batch: all workers panic.
	var ag1 workshare.AsyncGroup[*req]

	for range 2 {
		if ag1.CanShareWork(w) {
			ag1.RunAsync(w, func(_ *workshare.Pool[*req], _ *req) {
				panic("batch 1 panic")
			}, &req{})
		}
	}

	ag1.Wait()

	// Second batch: normal work — pool must still function after panics.
	var ag2 workshare.AsyncGroup[*req]

	for i := range 3 {
		if ag2.CanShareWork(w) {
			ag2.RunAsync(w, func(_ *workshare.Pool[*req], r *req) {
				r.result = 100
			}, &req{})
		} else {
			// If pool is full, do inline (expected for i >= 2 with 2 workers).
			_ = i
		}
	}

	for _, r := range ag2.Wait() {
		require.Equal(t, 100, r.result)
	}
}

var treeToWalk = buildTree(6)

func BenchmarkComputeTreeSum(b *testing.B) {
	w := workshare.NewPool[*computeTreeSumRequest](10)
	defer w.Close()

	for b.Loop() {
		computeTreeSum(w, treeToWalk)
	}
}
