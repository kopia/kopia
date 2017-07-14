package repo

import (
	"math"
	"math/rand"
	"testing"

	"github.com/chmduquesne/rollinghash/adler32"
	"github.com/chmduquesne/rollinghash/buzhash32"
	"github.com/chmduquesne/rollinghash/rabinkarp32"
)

func TestSplitters(t *testing.T) {
	cases := []struct {
		desc        string
		newSplitter func() objectSplitter
	}{
		{"rolling buzhash32 with 3 bits", func() objectSplitter { return newRollingHashSplitter(buzhash32.New(), 3) }},
		{"rolling adler32 with 5 bits", func() objectSplitter { return newRollingHashSplitter(adler32.New(), 5) }},
	}

	for _, tc := range cases {
		s1 := tc.newSplitter()
		s2 := tc.newSplitter()

		rnd := make([]byte, 50000000)
		rand.Read(rnd)

		for i, p := range rnd {
			if got, want := s1.add(p), s2.add(p); got != want {
				t.Errorf("incorrect add() result for %v at offset %v", tc.desc, i)
			}
		}
	}
}

func TestSplitterStability(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	rnd := make([]byte, 5000000)
	if n, err := r.Read(rnd); n != len(rnd) || err != nil {
		t.Fatalf("can't initialize random data: %v", err)
	}

	cases := []struct {
		splitter objectSplitter
		count    int
		avg      int
		minSplit int
		maxSplit int
	}{
		{newFixedSplitter(1000), 5000, 1000, 1000, 1000},
		{newFixedSplitter(10000), 500, 10000, 10000, 10000},

		{newNeverSplitter(), 0, 0, math.MaxInt32, 0},

		{newRollingHashSplitter(buzhash32.New(), 32), 156283, 31, 1, 427},
		{newRollingHashSplitter(buzhash32.New(), 1024), 4794, 1042, 1, 10001},
		{newRollingHashSplitter(buzhash32.New(), 2048), 2404, 2079, 1, 19312},
		{newRollingHashSplitter(buzhash32.New(), 32768), 143, 34965, 1, 233567},
		{newRollingHashSplitter(buzhash32.New(), 65536), 72, 69444, 1, 430586},

		{newRollingHashSplitter(rabinkarp32.New(), 32), 156303, 31, 1, 425},
		{newRollingHashSplitter(rabinkarp32.New(), 1024), 4985, 1003, 1, 9572},
		{newRollingHashSplitter(rabinkarp32.New(), 2048), 2497, 2002, 1, 15173},
		{newRollingHashSplitter(rabinkarp32.New(), 32768), 151, 33112, 790, 164382},
		{newRollingHashSplitter(rabinkarp32.New(), 65536), 76, 65789, 1124, 295680},
	}

	for _, tc := range cases {
		s := tc.splitter

		lastSplit := -1
		maxSplit := 0
		minSplit := int(math.MaxInt32)
		count := 0
		for i, p := range rnd {
			if s.add(p) {
				l := i - lastSplit
				if l >= maxSplit {
					maxSplit = l
				}
				if l < minSplit {
					minSplit = l
				}
				count++
				lastSplit = i
			}
		}

		var avg int
		if count > 0 {
			avg = len(rnd) / count
		}

		if got, want := avg, tc.avg; got != want {
			t.Errorf("invalid split average size %v, wanted %v", got, want)
		}

		if got, want := count, tc.count; got != want {
			t.Errorf("invalid split count %v, wanted %v", got, want)
		}
		if got, want := minSplit, tc.minSplit; got != want {
			t.Errorf("min split %v, wanted %v", got, want)
		}
		if got, want := maxSplit, tc.maxSplit; got != want {
			t.Errorf("max split %v, wanted %v", got, want)
		}
	}
}

func TestRollingHashBits(t *testing.T) {
	cases := []struct {
		blockSize int32
		bits      uint
	}{
		{256, 8},
		{128, 7},
		{100, 7},
		{500, 9},
		{700, 9},
		{724, 9},
		{725, 10},
		{768, 10},
		{1000, 10},
		{1000000, 20},
		{10000000, 23},
		{20000000, 24},
	}

	for _, tc := range cases {
		if got, want := rollingHashBits(tc.blockSize), tc.bits; got != want {
			t.Errorf("rollingHashBits(%v) = %v, wanted %v", tc.blockSize, got, want)
		}
	}
}
