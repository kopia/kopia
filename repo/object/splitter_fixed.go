package object

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

func newFixedSplitterFactory(length int) SplitterFactory {
	return func() Splitter {
		return &fixedSplitter{chunkLength: length}
	}
}
