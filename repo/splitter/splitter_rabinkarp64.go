package splitter

import "github.com/chmduquesne/rollinghash/rabinkarp64"

type rabinKarp64Splitter struct {
	// we're intentionally not using rollinghash.Hash32 interface because doing this in a tight loop
	// is 40% slower because compiler can't inline the call.
	rh      *rabinkarp64.RabinKarp64
	mask    uint64
	count   int
	minSize int
	maxSize int
}

func (rs *rabinKarp64Splitter) ShouldSplit(b byte) bool {
	rs.rh.Roll(b)
	rs.count++

	if rs.rh.Sum64()&rs.mask == 0 && rs.count >= rs.minSize {
		rs.count = 0
		return true
	}

	if rs.count >= rs.maxSize {
		rs.count = 0
		return true
	}

	return false
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
