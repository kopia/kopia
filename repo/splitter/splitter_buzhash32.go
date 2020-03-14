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
}

func (rs *buzhash32Splitter) ShouldSplit(b byte) bool {
	rs.rh.Roll(b)
	rs.count++

	if rs.rh.Sum32()&rs.mask == 0 && rs.count >= rs.minSize {
		rs.count = 0
		return true
	}

	if rs.count >= rs.maxSize {
		rs.count = 0
		return true
	}

	return false
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
