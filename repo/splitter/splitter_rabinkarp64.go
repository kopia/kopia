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

	// simple optimization, if we're below minSize, there's no reason to even
	// look at the Sum64() or maxSize
	if left := rs.minSize - rs.count - 1; left > 0 {
		fastPathBytes = left
		if fastPathBytes > len(b) {
			fastPathBytes = len(b)
		}

		var i int

		for i = 0; i < fastPathBytes-3; i += 4 {
			rs.rh.Roll(b[i])
			rs.rh.Roll(b[i+1])
			rs.rh.Roll(b[i+2])
			rs.rh.Roll(b[i+3])
		}

		for ; i < fastPathBytes; i++ {
			rs.rh.Roll(b[i])
		}

		rs.count += fastPathBytes
		b = b[fastPathBytes:]
	}

	// second optimization, if we're safely below maxSize, there's no reason to check it
	// in a loop
	if left := rs.maxSize - rs.count - 1; left > 0 {
		fp := left
		if fp >= len(b) {
			fp = len(b)
		}

		for i, b := range b[0:fp] {
			if rs.shouldSplitNoMax(b) {
				rs.count = 0
				return fastPathBytes + i + 1
			}
		}

		fastPathBytes += fp
		b = b[fp:]
	}

	for i, b := range b {
		if rs.shouldSplitWithMaxCheck(b) {
			rs.count = 0
			return fastPathBytes + i + 1
		}
	}

	return -1
}

func (rs *rabinKarp64Splitter) shouldSplitWithMaxCheck(b byte) bool {
	if rs.shouldSplitNoMax(b) || rs.count >= rs.maxSize {
		return true
	}

	return false
}

func (rs *rabinKarp64Splitter) shouldSplitNoMax(b byte) bool {
	rs.rh.Roll(b)
	rs.count++

	return rs.rh.Sum64()&rs.mask == 0
}

func (rs *rabinKarp64Splitter) MaxSegmentSize() int {
	return rs.maxSize
}

func newRabinKarp64SplitterFactory(avgSize int) Factory {
	mask := uint64(avgSize - 1)
	minSize, maxSize := avgSize/2, avgSize*2 //nolint:gomnd

	return func() Splitter {
		s := rabinkarp64.New()
		s.Write(make([]byte, splitterSlidingWindowSize)) //nolint:errcheck

		return &rabinKarp64Splitter{s, mask, 0, minSize, maxSize}
	}
}
