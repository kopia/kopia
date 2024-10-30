package cli

import (
	"context"
	"sort"

	atunits "github.com/alecthomas/units"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
)

type commandBenchmarkEncryption struct {
	blockSize            atunits.Base2Bytes
	repeat               int
	deprecatedAlgorithms bool
	optionPrint          bool
	parallel             int

	out textOutput
}

func (c *commandBenchmarkEncryption) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("encryption", "Run encryption benchmarks")
	cmd.Flag("block-size", "Size of a block to encrypt").Default("1MB").BytesVar(&c.blockSize)
	cmd.Flag("repeat", "Number of repetitions").Default("1000").IntVar(&c.repeat)
	cmd.Flag("deprecated", "Include deprecated algorithms").BoolVar(&c.deprecatedAlgorithms)
	cmd.Flag("parallel", "Number of parallel goroutines").Default("1").IntVar(&c.parallel)
	cmd.Flag("print-options", "Print out options usable for repository creation").BoolVar(&c.optionPrint)
	cmd.Action(svc.noRepositoryAction(c.run))
	c.out.setup(svc)
}

func (c *commandBenchmarkEncryption) run(ctx context.Context) error {
	results := c.runBenchmark(ctx)

	sort.Slice(results, func(i, j int) bool {
		return results[i].throughput > results[j].throughput
	})
	c.out.printStdout("     %-30v %v\n", "Encryption", "Throughput")
	c.out.printStdout("-----------------------------------------------------------------\n")

	for ndx, r := range results {
		c.out.printStdout("%3d. %-30v %v / second", ndx, r.encryption, units.BytesString(r.throughput))

		if c.optionPrint {
			c.out.printStdout(",   --encryption=%s", r.encryption)
		}

		c.out.printStdout("\n")
	}

	c.out.printStdout("-----------------------------------------------------------------\n")
	c.out.printStdout("Fastest option for this machine is: --encryption=%s\n", results[0].encryption)

	return nil
}

func (c *commandBenchmarkEncryption) runBenchmark(ctx context.Context) []cryptoBenchResult {
	var results []cryptoBenchResult

	data := make([]byte, c.blockSize)

	for _, ea := range encryption.SupportedAlgorithms(c.deprecatedAlgorithms) {
		enc, err := encryption.CreateEncryptor(&format.ContentFormat{
			Encryption: ea,
			Hash:       hashing.DefaultAlgorithm,
			MasterKey:  make([]byte, 32), //nolint:mnd
			HMACSecret: make([]byte, 32), //nolint:mnd
		})
		if err != nil {
			continue
		}

		log(ctx).Infof("Benchmarking encryption '%v'... (%v x %v bytes, parallelism %v)", ea, c.repeat, len(data), c.parallel)

		input := gather.FromSlice(data)
		tt := timetrack.Start()

		hashCount := c.repeat

		runInParallelNoInputNoResult(c.parallel, func() {
			var hashOutput [hashing.MaxHashSize]byte

			var encryptOutput gather.WriteBuffer
			defer encryptOutput.Close()

			for range hashCount {
				encryptOutput.Reset()

				if encerr := enc.Encrypt(input, hashOutput[:32], &encryptOutput); encerr != nil {
					log(ctx).Errorf("encryption failed: %v", encerr)
					break
				}
			}
		})

		_, bytesPerSecond := tt.Completed(float64(c.parallel) * float64(len(data)) * float64(hashCount))

		results = append(results, cryptoBenchResult{hash: "-", encryption: ea, throughput: bytesPerSecond})
	}

	return results
}
