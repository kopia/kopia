package cli

import (
	"sort"
	"time"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/content"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	benchmarkCryptoCommand    = benchmarkCommands.Command("crypto", "Run hash and encryption benchmarks")
	benchmarkCryptoBlockSize  = benchmarkCryptoCommand.Flag("block-size", "Size of a block to encrypt").Default("1MB").Bytes()
	benchmarkCryptoEncryption = benchmarkCryptoCommand.Flag("encryption", "Test encrypted formats").Default("true").Bool()
	benchmarkCryptoRepeat     = benchmarkCryptoCommand.Flag("repeat", "Number of repetitions").Default("100").Int()
)

func runBenchmarkCryptoAction(ctx *kingpin.ParseContext) error {

	type benchResult struct {
		hash       string
		encryption string
		throughput float64
	}
	var results []benchResult

	data := make([]byte, *benchmarkCryptoBlockSize)
	for _, ha := range content.SupportedHashAlgorithms() {
		for _, ea := range content.SupportedEncryptionAlgorithms() {
			isEncrypted := ea != "NONE"
			if *benchmarkCryptoEncryption != isEncrypted {
				continue
			}

			h, e, err := content.CreateHashAndEncryptor(content.FormattingOptions{
				Encryption: ea,
				Hash:       ha,
				MasterKey:  make([]byte, 32),
				HMACSecret: make([]byte, 32),
			})
			if err != nil {
				continue
			}

			log.Infof("Benchmarking hash '%v' and encryption '%v'... (%v x %v bytes)", ha, ea, *benchmarkCryptoRepeat, len(data))
			t0 := time.Now()
			hashCount := *benchmarkCryptoRepeat
			for i := 0; i < hashCount; i++ {
				contentID := h(data)
				if _, encerr := e.Encrypt(data, contentID); encerr != nil {
					log.Warningf("encryption failed: %v", encerr)
					break
				}
			}
			hashTime := time.Since(t0)
			bytesPerSecond := float64(len(data)) * float64(hashCount) / hashTime.Seconds()

			results = append(results, benchResult{hash: ha, encryption: ea, throughput: bytesPerSecond})
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].throughput > results[j].throughput
	})
	printStdout("     %-20v %-20v %v\n", "Hash", "Encryption", "Throughput")
	printStdout("-----------------------------------------------------------------\n")
	for ndx, r := range results {
		printStdout("%3d. %-20v %-20v %v / second\n", ndx, r.hash, r.encryption, units.BytesStringBase2(int64(r.throughput)))

	}
	return nil
}

func init() {
	benchmarkCryptoCommand.Action(runBenchmarkCryptoAction)
}
