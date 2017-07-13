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
		minSplit int
		maxSplit int
	}{
		{newFixedSplitter(1000), 5000, 1000, 1000},
		{newFixedSplitter(10000), 500, 10000, 10000},

		{newNeverSplitter(), 0, math.MaxInt32, 0},

		{newRollingHashSplitter(buzhash32.New(), 5), 156283, 1, 427},
		{newRollingHashSplitter(buzhash32.New(), 10), 4794, 1, 10001},
		{newRollingHashSplitter(buzhash32.New(), 11), 2404, 1, 19312},
		{newRollingHashSplitter(buzhash32.New(), 15), 143, 1, 233567},
		{newRollingHashSplitter(buzhash32.New(), 16), 72, 1, 430586},

		{newRollingHashSplitter(rabinkarp32.New(), 5), 156303, 1, 425},
		{newRollingHashSplitter(rabinkarp32.New(), 10), 4985, 1, 9572},
		{newRollingHashSplitter(rabinkarp32.New(), 11), 2497, 1, 15173},
		{newRollingHashSplitter(rabinkarp32.New(), 15), 151, 790, 164382},
		{newRollingHashSplitter(rabinkarp32.New(), 16), 76, 1124, 295680},
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
