package object

import (
	"math"

	"sort"

	"github.com/kopia/kopia/internal/config"
	"github.com/silvasur/buzhash"
)

type objectSplitter interface {
	add(b byte) bool
}

// SupportedSplitters is a list of supported object splitters including:
//
//    NEVER    - prevents objects from ever splitting
//    FIXED    - always splits large objects exactly at the maximum block size boundary
//    DYNAMIC  - dynamically splits large objects based on rolling hash of contents.
var SupportedSplitters []string

var splitterFactories = map[string]func(*config.RepositoryObjectFormat) objectSplitter{
	"NEVER": func(f *config.RepositoryObjectFormat) objectSplitter {
		return newNeverSplitter()
	},
	"FIXED": func(f *config.RepositoryObjectFormat) objectSplitter {
		return newFixedSplitter(f.MaxBlockSize)
	},
	"DYNAMIC": func(f *config.RepositoryObjectFormat) objectSplitter {
		return newRollingHashSplitter(buzhash.NewBuzHash(32), f.MinBlockSize, f.AvgBlockSize, f.MaxBlockSize)
	},
}

func init() {
	for k := range splitterFactories {
		SupportedSplitters = append(SupportedSplitters, k)
	}
	sort.Strings(SupportedSplitters)
}

// DefaultSplitter is the name of the splitter used by default for new repositories.
const DefaultSplitter = "DYNAMIC"

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

type rollingHash interface {
	HashByte(b byte) uint32
}

type rollingHashSplitter struct {
	rh   rollingHash
	mask uint32

	currentBlockSize int
	minBlockSize     int
	maxBlockSize     int
}

func (rs *rollingHashSplitter) add(b byte) bool {
	sum := rs.rh.HashByte(b)
	rs.currentBlockSize++
	if rs.currentBlockSize >= rs.maxBlockSize {
		rs.currentBlockSize = 0
		return true
	}
	if sum&rs.mask == 0 && rs.currentBlockSize > rs.minBlockSize && sum != 0 {
		//log.Printf("splitting %v on sum %x mask %x", rs.currentBlockSize, sum, rs.mask)
		rs.currentBlockSize = 0
		return true
	}
	return false
}

func newRollingHashSplitter(rh rollingHash, minBlockSize int, approxBlockSize int, maxBlockSize int) objectSplitter {
	bits := rollingHashBits(approxBlockSize)
	mask := ^(^uint32(0) << bits)
	return &rollingHashSplitter{rh, mask, 0, minBlockSize, maxBlockSize}
}

func rollingHashBits(n int) uint {
	e := math.Log2(float64(n))
	exp := math.Floor(e + 0.5)
	return uint(exp)
}
