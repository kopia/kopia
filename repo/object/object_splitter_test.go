package object

import (
	"math"
	"math/rand"
	"testing"
)

func TestSplitters(t *testing.T) {
	cases := []struct {
		desc        string
		newSplitter func() Splitter
	}{
		// {"rolling buzhash with 3 bits", func() Splitter { return newRollingHashSplitter(buzhash.NewBuzHash(32), 0, 8, 20) }},
		// {"rolling buzhash with 5 bits", func() Splitter { return newRollingHashSplitter(buzhash.NewBuzHash(32), 0, 32, 20) }},
	}

	for _, tc := range cases {
		s1 := tc.newSplitter()
		s2 := tc.newSplitter()

		rnd := make([]byte, 50000000)
		rand.Read(rnd)

		for i, p := range rnd {
			if got, want := s1.ShouldSplit(p), s2.ShouldSplit(p); got != want {
				t.Errorf("incorrect ShouldSplit() result for %v at offset %v", tc.desc, i)
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
		splitter Splitter
		count    int
		avg      int
		minSplit int
		maxSplit int
	}{
		// {newFixedSplitter(1000), 5000, 1000, 1000, 1000},
		// {newFixedSplitter(10000), 500, 10000, 10000, 10000},

		// {newNeverSplitter(), 0, 0, math.MaxInt32, 0},

		// {newRollingHashSplitter(buzhash.NewBuzHash(32), 0, 32, math.MaxInt32), 156262, 31, 1, 404},
		// {newRollingHashSplitter(buzhash.NewBuzHash(32), 0, 1024, math.MaxInt32), 4933, 1013, 1, 8372},
		// {newRollingHashSplitter(buzhash.NewBuzHash(32), 0, 2048, math.MaxInt32), 2476, 2019, 1, 19454},
		// {newRollingHashSplitter(buzhash.NewBuzHash(32), 0, 32768, math.MaxInt32), 185, 27027, 1, 177510},
		// {newRollingHashSplitter(buzhash.NewBuzHash(32), 0, 65536, math.MaxInt32), 99, 50505, 418, 230449},

		// // min and max
		// {newRollingHashSplitter(buzhash.NewBuzHash(32), 0, 32, 64), 179921, 27, 1, 64},
		// {newRollingHashSplitter(buzhash.NewBuzHash(32), 0, 1024, 10000), 4933, 1013, 1, 8372},
		// {newRollingHashSplitter(buzhash.NewBuzHash(32), 0, 2048, 10000), 2490, 2008, 1, 10000},
		// {newRollingHashSplitter(buzhash.NewBuzHash(32), 500, 32768, 100000), 183, 27322, 522, 100000},
		// {newRollingHashSplitter(buzhash.NewBuzHash(32), 500, 65536, 100000), 113, 44247, 522, 100000},
	}

	for _, tc := range cases {
		s := tc.splitter

		lastSplit := -1
		maxSplit := 0
		minSplit := int(math.MaxInt32)
		count := 0
		for i, p := range rnd {
			if s.ShouldSplit(p) {
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
