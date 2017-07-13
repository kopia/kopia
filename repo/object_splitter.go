package repo

import "github.com/chmduquesne/rollinghash"

type objectSplitter interface {
	add(b byte) bool
}

type neverSplitter struct{}

func (s *neverSplitter) add(b byte) bool {
	return false
}

func newNeverSplitter() objectSplitter {
	return &neverSplitter{}
}

type fixedSplitter struct {
	cur         int
	chunkLength int
}

func (s *fixedSplitter) add(b byte) bool {
	s.cur++
	if s.cur >= s.chunkLength {
		s.cur = 0
		return true
	}

	return false
}

func newFixedSplitter(chunkLength int) objectSplitter {
	return &fixedSplitter{chunkLength: chunkLength}
}

type rollingHashSplitter struct {
	rh      rollinghash.Hash32
	mask    uint32
	allOnes uint32
}

func (rs *rollingHashSplitter) add(b byte) bool {
	rs.rh.Roll(b)
	return rs.rh.Sum32()&rs.mask == rs.allOnes
}

func newRollingHashSplitter(rh rollinghash.Hash32, bits uint) objectSplitter {
	mask := ^(^uint32(0) << bits)
	return &rollingHashSplitter{rh, mask, (uint32(0)) ^ mask}
}
