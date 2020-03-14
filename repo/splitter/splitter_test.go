package splitter

import (
	cryptorand "crypto/rand"
	"math"
	"math/rand"
	"testing"
)

func TestSplitters(t *testing.T) {
	cases := []struct {
		desc        string
		newSplitter Factory
	}{
		{"rolling buzhash with 3 bits", newBuzHash32SplitterFactory(8)},
		{"rolling buzhash with 5 bits", newBuzHash32SplitterFactory(32)},
	}

	for _, tc := range cases {
		s1 := tc.newSplitter()
		s2 := tc.newSplitter()

		rnd := make([]byte, 50000000)
		cryptorand.Read(rnd) //nolint:errcheck

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
		{&fixedSplitter{0, 1000}, 5000, 1000, 1000, 1000},
		{&fixedSplitter{0, 10000}, 500, 10000, 10000, 10000},

		{newBuzHash32SplitterFactory(32)(), 124235, 40, 16, 64},
		{newBuzHash32SplitterFactory(1024)(), 3835, 1303, 512, 2048},
		{newBuzHash32SplitterFactory(2048)(), 1924, 2598, 1024, 4096},
		{newBuzHash32SplitterFactory(32768)(), 112, 44642, 16413, 65536},
		{newBuzHash32SplitterFactory(65536)(), 57, 87719, 32932, 131072},
		{newRabinKarp64SplitterFactory(32)(), 124108, 40, 16, 64},
		{newRabinKarp64SplitterFactory(1024)(), 3771, 1325, 512, 2048},
		{newRabinKarp64SplitterFactory(2048)(), 1887, 2649, 1028, 4096},
		{newRabinKarp64SplitterFactory(32768)(), 121, 41322, 16896, 65536},
		{newRabinKarp64SplitterFactory(65536)(), 53, 94339, 35875, 131072},
	}

	for _, tc := range cases {
		s := tc.splitter

		lastSplit := -1
		maxSplit := 0
		minSplit := int(math.MaxInt32)
		count := 0

		if got, want := s.MaxSegmentSize(), tc.maxSplit; got != want {
			t.Errorf("unexpected max segment size: %v, want %v", got, want)
		}

		for i, p := range rnd {
			if !s.ShouldSplit(p) {
				continue
			}

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
