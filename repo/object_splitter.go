package repo

import (
	"math"

	"github.com/chmduquesne/rollinghash"
	"github.com/chmduquesne/rollinghash/buzhash32"
)

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

	currentBlockSize int32
	minBlockSize     int32
	maxBlockSize     int32
}

func (rs *rollingHashSplitter) add(b byte) bool {
	rs.rh.Roll(b)
	rs.currentBlockSize++
	if rs.currentBlockSize < rs.minBlockSize {
		return false
	}
	if rs.currentBlockSize >= rs.maxBlockSize {
		rs.currentBlockSize = 0
		return true
	}
	if rs.rh.Sum32()&rs.mask == rs.allOnes {
		rs.currentBlockSize = 0
		return true
	}
	return false
}

func newRollingHashSplitter(rh rollinghash.Hash32, minBlockSize int32, approxBlockSize int32, maxBlockSize int32) objectSplitter {
	bits := rollingHashBits(approxBlockSize)
	mask := ^(^uint32(0) << bits)
	return &rollingHashSplitter{rh, mask, (uint32(0)) ^ mask, 0, minBlockSize, maxBlockSize}
}

func rollingHashBits(n int32) uint {
	e := math.Log2(float64(n))
	exp := math.Floor(e + 0.5)
	return uint(exp)
}

//SupportedSplitters is a map of supported splitters their factory functions.
var SupportedSplitters = map[string]func(*Format) objectSplitter{
	"NEVER": func(f *Format) objectSplitter {
		return newNeverSplitter()
	},
	"FIXED": func(f *Format) objectSplitter {
		return newFixedSplitter(int(f.MaxBlockSize))
	},
	"DYNAMIC": func(f *Format) objectSplitter {
		return newRollingHashSplitter(buzhash32.New(), f.MinBlockSize, f.ApproxBlockSize, f.MaxBlockSize)
	},
}
