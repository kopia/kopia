package repo

import (
	"math"

	"sort"

	"github.com/chmduquesne/rollinghash"
	"github.com/chmduquesne/rollinghash/buzhash32"
	"github.com/kopia/kopia/internal/config"
)

type objectSplitter interface {
	add(b byte) bool
}

// SupportedObjectSplitters is a list of supported object splitters including:
//
//    NEVER    - prevents objects from ever splitting
//    FIXED    - always splits large objects exactly at the maximum block size boundary
//    DYNAMIC  - dynamicaly splits large objects based on rolling hash of contents.
var SupportedObjectSplitters []string

var objectSplitterFactories = map[string]func(*config.RepositoryObjectFormat) objectSplitter{
	"NEVER": func(f *config.RepositoryObjectFormat) objectSplitter {
		return newNeverSplitter()
	},
	"FIXED": func(f *config.RepositoryObjectFormat) objectSplitter {
		return newFixedSplitter(int(f.MaxBlockSize))
	},
	"DYNAMIC": func(f *config.RepositoryObjectFormat) objectSplitter {
		return newRollingHashSplitter(buzhash32.New(), f.MinBlockSize, f.AvgBlockSize, f.MaxBlockSize)
	},
}

func init() {
	for k := range objectSplitterFactories {
		SupportedObjectSplitters = append(SupportedObjectSplitters, k)
	}
	sort.Strings(SupportedObjectSplitters)
}

// DefaultObjectSplitter is the name of the splitter used by default for new repositories.
const DefaultObjectSplitter = "DYNAMIC"

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

	currentBlockSize int
	minBlockSize     int
	maxBlockSize     int
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

func newRollingHashSplitter(rh rollinghash.Hash32, minBlockSize int, approxBlockSize int, maxBlockSize int) objectSplitter {
	bits := rollingHashBits(approxBlockSize)
	mask := ^(^uint32(0) << bits)
	return &rollingHashSplitter{rh, mask, (uint32(0)) ^ mask, 0, minBlockSize, maxBlockSize}
}

func rollingHashBits(n int) uint {
	e := math.Log2(float64(n))
	exp := math.Floor(e + 0.5)
	return uint(exp)
}
