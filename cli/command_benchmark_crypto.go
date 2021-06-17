package cli

import (
	"context"
	"sort"

	atunits "github.com/alecthomas/units"

	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

type commandBenchmarkCrypto struct {
	blockSize            atunits.Base2Bytes
	repeat               int
	deprecatedAlgorithms bool
	optionPrint          bool

	out textOutput
}

func (c *commandBenchmarkCrypto) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("crypto", "Run hash and encryption benchmarks")
	cmd.Flag("block-size", "Size of a block to encrypt").Default("1MB").BytesVar(&c.blockSize)
	cmd.Flag("repeat", "Number of repetitions").Default("100").IntVar(&c.repeat)
	cmd.Flag("deprecated", "Include deprecated algorithms").BoolVar(&c.deprecatedAlgorithms)
	cmd.Flag("print-options", "Print out options usable for repository creation").BoolVar(&c.optionPrint)
	cmd.Action(svc.noRepositoryAction(c.run))
	c.out.setup(svc)
}

func (c *commandBenchmarkCrypto) run(ctx context.Context) error {
	type benchResult struct {
		hash       string
		encryption string
		throughput float64
	}

	var results []benchResult

	data := make([]byte, c.blockSize)

	const (
		maxEncryptionOverhead = 1024
	)

	var hashOutput [hashing.MaxHashSize]byte

	encryptOutput := make([]byte, len(data)+maxEncryptionOverhead)

	for _, ha := range hashing.SupportedAlgorithms() {
		for _, ea := range encryption.SupportedAlgorithms(c.deprecatedAlgorithms) {
			cr, err := content.CreateCrypter(&content.FormattingOptions{
				Encryption: ea,
				Hash:       ha,
				MasterKey:  make([]byte, 32), // nolint:gomnd
				HMACSecret: make([]byte, 32), // nolint:gomnd
			})
			if err != nil {
				continue
			}

			log(ctx).Infof("Benchmarking hash '%v' and encryption '%v'... (%v x %v bytes)", ha, ea, c.repeat, len(data))

			tt := timetrack.Start()

			hashCount := c.repeat

			for i := 0; i < hashCount; i++ {
				contentID := cr.HashFunction(hashOutput[:0], data)
				if _, encerr := cr.Encryptor.Encrypt(encryptOutput[:0], data, contentID); encerr != nil {
					log(ctx).Errorf("encryption failed: %v", encerr)
					break
				}
			}

			_, bytesPerSecond := tt.Completed(float64(len(data)) * float64(hashCount))

			results = append(results, benchResult{hash: ha, encryption: ea, throughput: bytesPerSecond})
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].throughput > results[j].throughput
	})
	c.out.printStdout("     %-20v %-20v %v\n", "Hash", "Encryption", "Throughput")
	c.out.printStdout("-----------------------------------------------------------------\n")

	for ndx, r := range results {
		c.out.printStdout("%3d. %-20v %-20v %v / second", ndx, r.hash, r.encryption, units.BytesStringBase2(int64(r.throughput)))

		if c.optionPrint {
			c.out.printStdout(",   --block-hash=%s --encryption=%s", r.hash, r.encryption)
		}

		c.out.printStdout("\n")
	}

	c.out.printStdout("-----------------------------------------------------------------\n")
	c.out.printStdout("Fastest option for this machine is: --block-hash=%s --encryption=%s\n", results[0].hash, results[0].encryption)

	return nil
}
