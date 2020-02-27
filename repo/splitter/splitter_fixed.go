package splitter

type fixedSplitter struct {
	cur         int
	chunkLength int
}

func (s *fixedSplitter) ShouldSplit(b byte) bool {
	s.cur++

	if s.cur >= s.chunkLength {
		s.cur = 0
		return true
	}

	return false
}

// Fixed returns a factory that creates splitters with fixed chunk length.
func Fixed(length int) Factory {
	return func() Splitter {
		return &fixedSplitter{chunkLength: length}
	}
}
