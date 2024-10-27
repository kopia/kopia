package cli

import (
	"context"
	"sort"

	atunits "github.com/alecthomas/units"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
)

type commandBenchmarkHashing struct {
	blockSize   atunits.Base2Bytes
	repeat      int
	optionPrint bool
	parallel    int

	out textOutput
}

func (c *commandBenchmarkHashing) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("hashing", "Run hashing function benchmarks").Alias("hash")
	cmd.Flag("block-size", "Size of a block to hash").Default("1MB").BytesVar(&c.blockSize)
	cmd.Flag("repeat", "Number of repetitions").Default("10").IntVar(&c.repeat)
	cmd.Flag("parallel", "Number of parallel goroutines").Default("1").IntVar(&c.parallel)
	cmd.Flag("print-options", "Print out options usable for repository creation").BoolVar(&c.optionPrint)
	cmd.Action(svc.noRepositoryAction(c.run))
	c.out.setup(svc)
}

func (c *commandBenchmarkHashing) run(ctx context.Context) error {
	results := c.runBenchmark(ctx)

	sort.Slice(results, func(i, j int) bool {
		return results[i].throughput > results[j].throughput
	})
	c.out.printStdout("     %-20v %v\n", "Hash", "Throughput")
	c.out.printStdout("-----------------------------------------------------------------\n")

	for ndx, r := range results {
		c.out.printStdout("%3d. %-20v %v / second", ndx, r.hash, units.BytesString(r.throughput))

		if c.optionPrint {
			c.out.printStdout(",   --block-hash=%s", r.hash)
		}

		c.out.printStdout("\n")
	}

	c.out.printStdout("-----------------------------------------------------------------\n")
	c.out.printStdout("Fastest option for this machine is: --block-hash=%s\n", results[0].hash)

	return nil
}

func (c *commandBenchmarkHashing) runBenchmark(ctx context.Context) []cryptoBenchResult {
	var results []cryptoBenchResult

	data := make([]byte, c.blockSize)

	for _, ha := range hashing.SupportedAlgorithms() {
		hf, err := hashing.CreateHashFunc(&format.ContentFormat{
			Hash:       ha,
			HMACSecret: make([]byte, 32), //nolint:mnd
		})
		if err != nil {
			continue
		}

		log(ctx).Infof("Benchmarking hash '%v' (%v x %v bytes, parallelism %v)", ha, c.repeat, len(data), c.parallel)

		input := gather.FromSlice(data)
		tt := timetrack.Start()

		hashCount := c.repeat

		runInParallelNoInputNoResult(c.parallel, func() {
			var hashOutput [hashing.MaxHashSize]byte

			for range hashCount {
				for range hashOutput {
					hf(hashOutput[:0], input)
				}
			}
		})

		_, bytesPerSecond := tt.Completed(float64(c.parallel) * float64(len(data)) * float64(hashCount))

		results = append(results, cryptoBenchResult{hash: ha, encryption: "-", throughput: bytesPerSecond})
	}

	return results
}
