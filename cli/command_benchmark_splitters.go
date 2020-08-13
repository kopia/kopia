package cli

import (
	"math/rand"
	"sort"
	"time"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/kopia/kopia/repo/splitter"
)

var (
	benchmarkSplitterCommand    = benchmarkCommands.Command("splitter", "Run splitter benchmarks")
	benchmarkSplitterRandSeed   = benchmarkSplitterCommand.Flag("rand-seed", "Random seed").Default("42").Int64()
	benchmarkSplitterBlockSize  = benchmarkSplitterCommand.Flag("data-size", "Size of a data to split").Default("32MB").Bytes()
	benchmarkSplitterBlockCount = benchmarkSplitterCommand.Flag("block-count", "Number of data blocks to split").Default("16").Int()
)

func runBenchmarkSplitterAction(ctx *kingpin.ParseContext) error {
	type benchResult struct {
		splitter     string
		duration     time.Duration
		segmentCount int
		min          int
		p10          int
		p25          int
		p50          int
		p75          int
		p90          int
		max          int
	}

	var results []benchResult

	// generate data blocks
	var dataBlocks [][]byte

	rnd := rand.New(rand.NewSource(*benchmarkSplitterRandSeed)) //nolint:gosec

	for i := 0; i < *benchmarkSplitterBlockCount; i++ {
		b := make([]byte, *benchmarkSplitterBlockSize)
		if _, err := rnd.Read(b); err != nil {
			return err
		}

		dataBlocks = append(dataBlocks, b)
	}

	printStderr("splitting %v blocks of %v each\n", *benchmarkSplitterBlockCount, *benchmarkSplitterBlockSize)

	for _, sp := range splitter.SupportedAlgorithms() {
		fact := splitter.GetFactory(sp)

		var segmentLengths []int

		t0 := time.Now()

		for _, data := range dataBlocks {
			s := fact()
			l := 0

			for _, d := range data {
				l++

				if s.ShouldSplit(d) {
					segmentLengths = append(segmentLengths, l)
					l = 0
				}
			}

			if l > 0 {
				segmentLengths = append(segmentLengths, l)
			}
		}

		dur := time.Since(t0)

		sort.Ints(segmentLengths)

		r := benchResult{
			sp,
			dur,
			len(segmentLengths),
			segmentLengths[0],
			segmentLengths[len(segmentLengths)*10/100],
			segmentLengths[len(segmentLengths)*25/100],
			segmentLengths[len(segmentLengths)*50/100],
			segmentLengths[len(segmentLengths)*75/100],
			segmentLengths[len(segmentLengths)*90/100],
			segmentLengths[len(segmentLengths)-1],
		}

		printStdout("%-25v %6v ms count:%v min:%v 10th:%v 25th:%v 50th:%v 75th:%v 90th:%v max:%v\n",
			r.splitter,
			r.duration.Nanoseconds()/1e6,
			r.segmentCount,
			r.min, r.p10, r.p25, r.p50, r.p75, r.p90, r.max)

		results = append(results, r)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].duration < results[j].duration
	})
	printStdout("-----------------------------------------------------------------\n")

	for ndx, r := range results {
		printStdout("%3v. %-25v %6v ms count:%v min:%v 10th:%v 25th:%v 50th:%v 75th:%v 90th:%v max:%v\n",
			ndx,
			r.splitter,
			r.duration.Nanoseconds()/1e6,
			r.segmentCount,
			r.min, r.p10, r.p25, r.p50, r.p75, r.p90, r.max)
	}

	return nil
}

func init() {
	benchmarkSplitterCommand.Action(runBenchmarkSplitterAction)
}
