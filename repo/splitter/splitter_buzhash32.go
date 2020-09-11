package splitter

import (
	"github.com/chmduquesne/rollinghash/buzhash32"
)

type buzhash32Splitter struct {
	// we're intentionally not using rollinghash.Hash32 interface because doing this in a tight loop
	// is 40% slower because compiler can't inline the call.
	rh      *buzhash32.Buzhash32
	mask    uint32
	count   int
	minSize int
	maxSize int
}

func (rs *buzhash32Splitter) Close() {
}

func (rs *buzhash32Splitter) Reset() {
	rs.rh.Reset()
	rs.rh.Write(make([]byte, splitterSlidingWindowSize)) //nolint:errcheck
	rs.count = 0
}

func (rs *buzhash32Splitter) NextSplitPoint(b []byte) int {
	var fastPathBytes int

	// simple optimization, if we're below minSize, there's no reason to even
	// look at the Sum32() or maxSize
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

func (rs *buzhash32Splitter) shouldSplitWithMaxCheck(b byte) bool {
	if rs.shouldSplitNoMax(b) || rs.count >= rs.maxSize {
		return true
	}

	return false
}

func (rs *buzhash32Splitter) shouldSplitNoMax(b byte) bool {
	rs.rh.Roll(b)
	rs.count++

	return rs.rh.Sum32()&rs.mask == 0
}

func (rs *buzhash32Splitter) MaxSegmentSize() int {
	return rs.maxSize
}

func newBuzHash32SplitterFactory(avgSize int) Factory {
	// avgSize must be a power of two, so 0b000001000...0000
	// it just so happens that mask is avgSize-1 :)
	mask := uint32(avgSize - 1)
	maxSize := avgSize * 2 // nolint:gomnd
	minSize := avgSize / 2 // nolint:gomnd

	return func() Splitter {
		s := buzhash32.New()
		s.Write(make([]byte, splitterSlidingWindowSize)) //nolint:errcheck

		return &buzhash32Splitter{s, mask, 0, minSize, maxSize}
	}
}
