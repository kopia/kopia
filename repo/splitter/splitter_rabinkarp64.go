package splitter

import (
	"github.com/chmduquesne/rollinghash/rabinkarp64"
)

type rabinKarp64Splitter struct {
	// we're intentionally not using rollinghash.Hash32 interface because doing this in a tight loop
	// is 40% slower because compiler can't inline the call.
	rh      *rabinkarp64.RabinKarp64
	mask    uint64
	count   int
	minSize int
	maxSize int
}

func (rs *rabinKarp64Splitter) Close() {
}

func (rs *rabinKarp64Splitter) Reset() {
	rs.rh.Reset()
	rs.rh.Write(make([]byte, splitterSlidingWindowSize)) //nolint:errcheck
	rs.count = 0
}

func (rs *rabinKarp64Splitter) NextSplitPoint(b []byte) int {
	var fastPathBytes int

	// until minSize, only hash the last splitterSlidingWindowSize bytes
	if left := rs.minSize - rs.count - 1; left > 0 {
		fastPathBytes = left
		if fastPathBytes > len(b) {
			fastPathBytes = len(b)
		}

		var i int

		i = fastPathBytes - splitterSlidingWindowSize
		if i < 0 {
			i = 0
		}

		for ; i < fastPathBytes; i++ {
			rs.rh.Roll(b[i])
		}

		rs.count += fastPathBytes
		b = b[fastPathBytes:]
	}

	// until the max size, check if we have any splitting point
	if left := rs.maxSize - rs.count; left > 0 {
		fp := left
		if fp >= len(b) {
			fp = len(b)
		}

		for i, b := range b[0:fp] {
			rs.rh.Roll(b)

			rs.count++

			if rs.rh.Sum64()&rs.mask == 0 {
				rs.count = 0
				return fastPathBytes + i + 1
			}
		}

		fastPathBytes += fp
	}

	// if we're over the max size, split
	if rs.count >= rs.maxSize {
		rs.count = 0
		return fastPathBytes
	}

	return -1
}

func (rs *rabinKarp64Splitter) MaxSegmentSize() int {
	return rs.maxSize
}

func newRabinKarp64SplitterFactory(avgSize int) Factory {
	mask := uint64(avgSize - 1)              //nolint:gosec
	minSize, maxSize := avgSize/2, avgSize*2 //nolint:mnd

	return func() Splitter {
		s := rabinkarp64.New()
		s.Write(make([]byte, splitterSlidingWindowSize)) //nolint:errcheck

		return &rabinKarp64Splitter{s, mask, 0, minSize, maxSize}
	}
}
