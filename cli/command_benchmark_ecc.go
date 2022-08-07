package cli

import (
	"context"
	"fmt"
	"math"
	"sort"

	atunits "github.com/alecthomas/units"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/ecc"
)

type commandBenchmarkEcc struct {
	blockSize   atunits.Base2Bytes
	repeat      int
	optionPrint bool
	parallel    int

	out textOutput
}

func (c *commandBenchmarkEcc) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("ecc", "Run ECC benchmarks")
	cmd.Flag("block-size", "Size of a block to encrypt").Default("10MB").BytesVar(&c.blockSize)
	cmd.Flag("repeat", "Number of repetitions").Default("100").IntVar(&c.repeat)
	cmd.Flag("parallel", "Number of parallel goroutines").Default("1").IntVar(&c.parallel)
	cmd.Flag("print-options", "Print out options usable for repository creation").BoolVar(&c.optionPrint)
	cmd.Action(svc.noRepositoryAction(c.run))
	c.out.setup(svc)
}

func (c *commandBenchmarkEcc) run(ctx context.Context) error {
	results := c.runBenchmark(ctx)

	sort.Slice(results, func(i, j int) bool {
		return min(results[i].throughputEncoding, results[i].throughputDecoding) >
			min(results[j].throughputEncoding, results[j].throughputDecoding)
	})

	c.out.printStdout("     %-30v %14v %14v   %10v\n", "ECC", "Throughput", "Throughput", "Growth")
	c.out.printStdout("     %-30v %14v %14v   %6v\n", "", "Encoding", "Decoding", "")
	c.out.printStdout("---------------------------------------------------------------------------------------\n")

	for ndx, r := range results {
		c.out.printStdout("%3d. %-30v %12v/s %12v/s   %6v%% [%v]", ndx, r.ecc,
			units.BytesStringBase2(int64(r.throughputEncoding)),
			units.BytesStringBase2(int64(r.throughputDecoding)),
			int(math.Round(r.growth*100)), //nolint:gomnd
			units.BytesStringBase2(int64(r.size)),
		)

		if c.optionPrint {
			c.out.printStdout(",   --ecc=%s", r.ecc)
		}

		c.out.printStdout("\n")
	}

	c.out.printStdout("---------------------------------------------------------------------------------------\n")
	c.out.printStdout("Fastest option for this machine is: --ecc=%s\n", results[0].ecc)

	return nil
}

func (c *commandBenchmarkEcc) runBenchmark(ctx context.Context) []eccBenchResult {
	var results []eccBenchResult

	data := make([]byte, c.blockSize)
	for i := uint64(0); i < uint64(c.blockSize); i++ {
		data[i] = byte(i%255 + 1)
	}

	var encodedBuffer gather.WriteBuffer
	defer encodedBuffer.Close()

	for _, name := range ecc.SupportedAlgorithms() {
		for _, spaceOverhead := range []uint8{1, 2, 5, 10, 20} {
			impl, err := ecc.CreateAlgorithm(&ecc.Options{
				Algorithm:                ecc.AlgorithmReedSolomonWithCrc32,
				SpaceOverhead:            spaceOverhead,
				DeleteFirstShardForTests: true,
			})
			if err != nil {
				continue
			}

			log(ctx).Infof("Benchmarking ECC encoding '%v' with %v space overhead... (%v x %v bytes, parallelism %v)", name, spaceOverhead, c.repeat, len(data), c.parallel)

			input := gather.FromSlice(data)
			tt := timetrack.Start()

			repeat := c.repeat

			runInParallel(c.parallel, func() interface{} {
				var tmp gather.WriteBuffer
				defer tmp.Close()

				for i := 0; i < repeat; i++ {
					if encerr := impl.Encrypt(input, nil, &tmp); encerr != nil {
						log(ctx).Errorf("encoding failed: %v", encerr)
						break
					}
				}

				return nil
			})

			_, bytesPerSecondEncoding := tt.Completed(float64(c.parallel) * float64(len(data)) * float64(repeat))

			log(ctx).Infof("Benchmarking ECC decoding '%v' with %v space overhead... (%v x %v bytes, parallelism %v)", name, spaceOverhead, c.repeat, len(data), c.parallel)

			encodedBuffer.Reset()

			if err := impl.Encrypt(gather.FromSlice(data), nil, &encodedBuffer); err != nil {
				log(ctx).Errorf("encoding failed: %v", err)
				break
			}

			input = encodedBuffer.Bytes()
			tt = timetrack.Start()

			runInParallel(c.parallel, func() interface{} {
				var tmp gather.WriteBuffer
				defer tmp.Close()

				for i := 0; i < repeat; i++ {
					if decerr := impl.Decrypt(input, nil, &tmp); decerr != nil {
						log(ctx).Errorf("decoding failed: %v", decerr)
						break
					}
				}

				return nil
			})

			_, bytesPerSecondDecoding := tt.Completed(float64(c.parallel) * float64(len(data)) * float64(repeat))

			results = append(results, eccBenchResult{
				ecc:                fmt.Sprintf("%v - %v%%", name, spaceOverhead),
				throughputEncoding: bytesPerSecondEncoding,
				throughputDecoding: bytesPerSecondDecoding,
				size:               input.Length(),
				growth:             float64(input.Length())/float64(c.blockSize) - 1,
			})
		}
	}

	return results
}

type eccBenchResult struct {
	ecc                string
	throughputEncoding float64
	throughputDecoding float64
	size               int
	growth             float64
}

func min(a, b float64) float64 {
	if a <= b {
		return a
	}

	return b
}
