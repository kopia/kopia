package splitter

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/kopia/kopia/internal/testutil"
)

func TestSplitterStability(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	rnd := make([]byte, 5000000)

	if n, err := r.Read(rnd); n != len(rnd) || err != nil {
		t.Fatalf("can't initialize random data: %v", err)
	}

	cases := []struct {
		factory  Factory
		count    int
		avg      int
		minSplit int
		maxSplit int
	}{
		{Fixed(1000), 5000, 1000, 1000, 1000},
		{Fixed(10000), 500, 10000, 10000, 10000},

		{newBuzHash32SplitterFactory(32), 124235, 40, 16, 64},
		{newBuzHash32SplitterFactory(1024), 3835, 1303, 512, 2048},
		{newBuzHash32SplitterFactory(2048), 1924, 2598, 1024, 4096},
		{newBuzHash32SplitterFactory(32768), 112, 44642, 16413, 65536},
		{newBuzHash32SplitterFactory(65536), 57, 87719, 32932, 131072},
		{newRabinKarp64SplitterFactory(32), 124108, 40, 16, 64},
		{newRabinKarp64SplitterFactory(1024), 3771, 1325, 512, 2048},
		{newRabinKarp64SplitterFactory(2048), 1887, 2649, 1028, 4096},
		{newRabinKarp64SplitterFactory(32768), 121, 41322, 16896, 65536},
		{newRabinKarp64SplitterFactory(65536), 53, 94339, 35875, 131072},

		{pooled(Fixed(1000)), 5000, 1000, 1000, 1000},

		{pooled(newBuzHash32SplitterFactory(32)), 124235, 40, 16, 64},
		{pooled(newBuzHash32SplitterFactory(1024)), 3835, 1303, 512, 2048},
		{pooled(newBuzHash32SplitterFactory(2048)), 1924, 2598, 1024, 4096},
		{pooled(newBuzHash32SplitterFactory(32768)), 112, 44642, 16413, 65536},
		{pooled(newBuzHash32SplitterFactory(65536)), 57, 87719, 32932, 131072},
		{pooled(newRabinKarp64SplitterFactory(32)), 124108, 40, 16, 64},
		{pooled(newRabinKarp64SplitterFactory(1024)), 3771, 1325, 512, 2048},
		{pooled(newRabinKarp64SplitterFactory(2048)), 1887, 2649, 1028, 4096},
		{pooled(newRabinKarp64SplitterFactory(32768)), 121, 41322, 16896, 65536},
		{pooled(newRabinKarp64SplitterFactory(65536)), 53, 94339, 35875, 131072},
	}

	// run each test twice to rule out the possibility of some state leaking through splitter reuse
	numRepeats := 2

	if testutil.ShouldReduceTestComplexity() {
		// on ARM pick random 1/4 of cases
		rand.Shuffle(len(cases), func(i, j int) {
			cases[i], cases[j] = cases[j], cases[i]
		})

		cases = cases[:len(cases)/4]
	}

	getSplitPointsFunctions := map[string]func(data []byte, s Splitter) (minSplit, maxSplit, count int){
		"getSplitPoints":             getSplitPoints,
		"getSplitPointsByteByByte":   getSplitPointsByteByByte,
		"getSplitPointsRandomSlices": getSplitPointsRandomSlices,
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%v", tc), func(t *testing.T) {
			t.Parallel()

			for name, getSplitPointsFunc := range getSplitPointsFunctions {
				t.Run(name, func(t *testing.T) {
					for range numRepeats {
						s := tc.factory()

						if got, want := s.MaxSegmentSize(), tc.maxSplit; got != want {
							t.Errorf("unexpected max segment size: %v, want %v", got, want)
						}

						minSplit, maxSplit, count := getSplitPointsFunc(rnd, s)

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

						// this returns the splitter back to the pool.
						s.Close()
					}
				})
			}
		})
	}
}

func getSplitPoints(data []byte, s Splitter) (minSplit, maxSplit, count int) {
	maxSplit = 0
	minSplit = int(math.MaxInt32)
	count = 0

	for len(data) > 0 {
		n := s.NextSplitPoint(data)
		if n < 0 {
			break
		}

		count++

		if n >= maxSplit {
			maxSplit = n
		}

		if n < minSplit {
			minSplit = n
		}

		data = data[n:]
	}

	return minSplit, maxSplit, count
}

func getSplitPointsByteByByte(data []byte, s Splitter) (minSplit, maxSplit, count int) {
	lastSplit := -1
	maxSplit = 0
	minSplit = int(math.MaxInt32)
	count = 0

	for i := range data {
		if s.NextSplitPoint(data[i:i+1]) == -1 {
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

	return minSplit, maxSplit, count
}

func getSplitPointsRandomSlices(data []byte, s Splitter) (minSplit, maxSplit, count int) {
	lastSplit := -1
	maxSplit = 0
	minSplit = int(math.MaxInt32)
	count = 0

	for i := 0; i < len(data); {
		// how many bytes to feed to the splitter.
		numBytes := rand.Intn(1000) + 1
		if i+numBytes > len(data) {
			numBytes = len(data) - i
		}

		n := s.NextSplitPoint(data[i : i+numBytes])
		if n == -1 {
			// no split point in the next numBytes bytes
			i += numBytes
			continue
		}

		// we have a split point and the splitter consumed 'n' bytes
		l := i + n - 1 - lastSplit
		if l >= maxSplit {
			maxSplit = l
		}

		if l < minSplit {
			minSplit = l
		}

		count++

		lastSplit = i + n - 1

		i += n
	}

	return minSplit, maxSplit, count
}
