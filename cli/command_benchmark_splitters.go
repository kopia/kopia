package cli

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	atunits "github.com/alecthomas/units"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/splitter"
)

type commandBenchmarkSplitters struct {
	randSeed    int64
	blockSize   atunits.Base2Bytes
	blockCount  int
	printOption bool
	parallel    int

	out textOutput
}

func (c *commandBenchmarkSplitters) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("splitter", "Run splitter benchmarks")

	cmd.Flag("rand-seed", "Random seed").Default("42").Int64Var(&c.randSeed)
	cmd.Flag("data-size", "Size of a data to split").Default("32MB").BytesVar(&c.blockSize)
	cmd.Flag("block-count", "Number of data blocks to split").Default("16").IntVar(&c.blockCount)
	cmd.Flag("print-options", "Print out the fastest dynamic splitter option").BoolVar(&c.printOption)
	cmd.Flag("parallel", "Number of parallel goroutines").Default("1").IntVar(&c.parallel)

	cmd.Action(svc.noRepositoryAction(c.run))

	c.out.setup(svc)
}

func (c *commandBenchmarkSplitters) run(ctx context.Context) error { //nolint:funlen
	type benchResult struct {
		splitter       string
		duration       time.Duration
		segmentCount   int
		min            int
		p10            int
		p25            int
		p50            int
		p75            int
		p90            int
		max            int
		bytesPerSecond int64
	}

	var results []benchResult

	var best benchResult

	best.duration = math.MaxInt64

	// generate data blocks
	var dataBlocks [][]byte

	rnd := rand.New(rand.NewSource(c.randSeed)) //nolint:gosec

	for range c.blockCount {
		b := make([]byte, c.blockSize)
		if _, err := rnd.Read(b); err != nil {
			return errors.Wrap(err, "error generating random data")
		}

		dataBlocks = append(dataBlocks, b)
	}

	log(ctx).Infof("splitting %v blocks of %v each, parallelism %v", c.blockCount, c.blockSize, c.parallel)

	for _, sp := range splitter.SupportedAlgorithms() {
		tt := timetrack.Start()

		segmentLengths := runInParallelNoInput(c.parallel, func() []int {
			fact := splitter.GetFactory(sp)

			var segmentLengths []int

			for _, d := range dataBlocks {
				s := fact()

				for len(d) > 0 {
					n := s.NextSplitPoint(d)
					if n < 0 {
						segmentLengths = append(segmentLengths, len(d))
						break
					}

					segmentLengths = append(segmentLengths, n)
					d = d[n:]
				}
			}

			return segmentLengths
		})

		_, bytesPerSecond := tt.Completed(float64(c.parallel) * float64(c.blockCount) * float64(c.blockSize))

		dur, _ := tt.Completed(0)

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
			int64(bytesPerSecond),
		}

		c.out.printStdout("%-25v %12v/s count:%v min:%v 10th:%v 25th:%v 50th:%v 75th:%v 90th:%v max:%v\n",
			r.splitter,
			units.BytesString(r.bytesPerSecond),
			r.segmentCount,
			r.min, r.p10, r.p25, r.p50, r.p75, r.p90, r.max,
		)

		results = append(results, r)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].duration < results[j].duration
	})
	c.out.printStdout("-----------------------------------------------------------------\n")

	for ndx, r := range results {
		c.out.printStdout("%3v. %-25v %-12v/s count:%v min:%v 10th:%v 25th:%v 50th:%v 75th:%v 90th:%v max:%v\n",
			ndx,
			r.splitter,
			units.BytesString(r.bytesPerSecond),
			r.segmentCount,
			r.min, r.p10, r.p25, r.p50, r.p75, r.p90, r.max)

		if best.duration > r.duration && !strings.HasPrefix(r.splitter, "FIXED") {
			best = r
		}
	}

	if c.printOption {
		c.out.printStdout("Fastest option for this machine is: --object-splitter=%s\n", best.splitter)
	}

	return nil
}
