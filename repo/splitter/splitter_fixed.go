package splitter

type fixedSplitter struct {
	cur         int
	chunkLength int
}

func (s *fixedSplitter) Close() {
}

func (s *fixedSplitter) Reset() {
	s.cur = 0
}

func (s *fixedSplitter) NextSplitPoint(b []byte) int {
	n := s.chunkLength - s.cur

	if len(b) < n {
		s.cur += len(b)
		return -1
	}

	s.cur = 0

	return n
}

func (s *fixedSplitter) MaxSegmentSize() int {
	return s.chunkLength
}

// Fixed returns a factory that creates splitters with fixed chunk length.
func Fixed(length int) Factory {
	return func() Splitter {
		return &fixedSplitter{chunkLength: length}
	}
}
