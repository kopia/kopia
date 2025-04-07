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

type commandBenchmarkCrypto struct {
	blockSize            atunits.Base2Bytes
	repeat               int
	deprecatedAlgorithms bool
	optionPrint          bool
	parallel             int

	out textOutput
}

func (c *commandBenchmarkCrypto) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("crypto", "Run combined hash and encryption benchmarks")
	cmd.Flag("block-size", "Size of a block to encrypt").Default("1MB").BytesVar(&c.blockSize)
	cmd.Flag("repeat", "Number of repetitions").Default("100").IntVar(&c.repeat)
	cmd.Flag("deprecated", "Include deprecated algorithms").BoolVar(&c.deprecatedAlgorithms)
	cmd.Flag("parallel", "Number of parallel goroutines").Default("1").IntVar(&c.parallel)
	cmd.Flag("print-options", "Print out options usable for repository creation").BoolVar(&c.optionPrint)
	cmd.Action(svc.noRepositoryAction(c.run))
	c.out.setup(svc)
}

func (c *commandBenchmarkCrypto) run(ctx context.Context) error {
	results := c.runBenchmark(ctx)

	sort.Slice(results, func(i, j int) bool {
		return results[i].throughput > results[j].throughput
	})
	c.out.printStdout("     %-20v %-30v %v\n", "Hash", "Encryption", "Throughput")
	c.out.printStdout("-----------------------------------------------------------------\n")

	for ndx, r := range results {
		c.out.printStdout("%3d. %-20v %-30v %v / second", ndx, r.hash, r.encryption, units.BytesString(r.throughput))

		if c.optionPrint {
			c.out.printStdout(",   --block-hash=%s --encryption=%s", r.hash, r.encryption)
		}

		c.out.printStdout("\n")
	}

	c.out.printStdout("-----------------------------------------------------------------\n")
	c.out.printStdout("Fastest option for this machine is: --block-hash=%s --encryption=%s\n", results[0].hash, results[0].encryption)

	return nil
}

func (c *commandBenchmarkCrypto) runBenchmark(ctx context.Context) []cryptoBenchResult {
	var results []cryptoBenchResult

	data := make([]byte, c.blockSize)

	for _, ha := range hashing.SupportedAlgorithms() {
		for _, ea := range encryption.SupportedAlgorithms(c.deprecatedAlgorithms) {
			fo := &format.ContentFormat{
				Encryption: ea,
				Hash:       ha,
				MasterKey:  make([]byte, 32), //nolint:mnd
				HMACSecret: make([]byte, 32), //nolint:mnd
			}

			hf, err := hashing.CreateHashFunc(fo)
			if err != nil {
				continue
			}

			enc, err := encryption.CreateEncryptor(fo)
			if err != nil {
				continue
			}

			log(ctx).Infof("Benchmarking hash '%v' and encryption '%v'... (%v x %v bytes, parallelism %v)", ha, ea, c.repeat, len(data), c.parallel)

			input := gather.FromSlice(data)
			tt := timetrack.Start()

			hashCount := c.repeat

			runInParallelNoInputNoResult(c.parallel, func() {
				var hashOutput [hashing.MaxHashSize]byte

				var encryptOutput gather.WriteBuffer
				defer encryptOutput.Close()

				for range hashCount {
					encryptOutput.Reset()

					contentID := hf(hashOutput[:0], input)

					if encerr := enc.Encrypt(input, contentID, &encryptOutput); encerr != nil {
						log(ctx).Errorf("encryption failed: %v", encerr)
						break
					}
				}
			})

			_, bytesPerSecond := tt.Completed(float64(c.parallel) * float64(len(data)) * float64(hashCount))

			results = append(results, cryptoBenchResult{hash: ha, encryption: ea, throughput: bytesPerSecond})
		}
	}

	return results
}
