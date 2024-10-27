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

			if rs.rh.Sum32()&rs.mask == 0 {
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

func (rs *buzhash32Splitter) MaxSegmentSize() int {
	return rs.maxSize
}

func newBuzHash32SplitterFactory(avgSize int) Factory {
	// avgSize must be a power of two, so 0b000001000...0000
	// it just so happens that mask is avgSize-1 :)
	mask := uint32(avgSize - 1) //nolint:gosec
	maxSize := avgSize * 2      //nolint:mnd
	minSize := avgSize / 2      //nolint:mnd

	return func() Splitter {
		s := buzhash32.New()
		s.Write(make([]byte, splitterSlidingWindowSize)) //nolint:errcheck

		return &buzhash32Splitter{s, mask, 0, minSize, maxSize}
	}
}
