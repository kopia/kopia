package cli

import (
	"sort"
	"time"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/repo/block"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	blockCryptoBenchmarkCommand    = blockCommands.Command("cryptobenchmark", "Run hash and encryption benchmarks")
	blockCryptoBenchmarkBlockSize  = blockCryptoBenchmarkCommand.Flag("block-size", "Size of a block to encrypt").Default("1MB").Bytes()
	blockCryptoBenchmarkEncryption = blockCryptoBenchmarkCommand.Flag("encryption", "Test encrypted formats").Default("true").Bool()
	blockCryptoBenchmarkRepeat     = blockCryptoBenchmarkCommand.Flag("repeat", "Number of repetitions").Default("100").Int()
)

type benchResult struct {
	hash       string
	encryption string
	throughput float64
}

func runBlockCryptoBenchmarkAction(ctx *kingpin.ParseContext) error {
	var results []benchResult

	data := make([]byte, *blockCryptoBenchmarkBlockSize)
	for _, ha := range block.SupportedHashAlgorithms() {
		for _, ea := range block.SupportedEncryptionAlgorithms() {
			isEncrypted := ea != "NONE"
			if *blockCryptoBenchmarkEncryption != isEncrypted {
				continue
			}

			h, e, err := block.CreateHashAndEncryptor(block.FormattingOptions{
				Encryption: ea,
				Hash:       ha,
				MasterKey:  make([]byte, 32),
				HMACSecret: make([]byte, 32),
			})
			if err != nil {
				continue
			}

			printStderr("Benchmarking hash '%v' and encryption '%v'...\n", ha, ea)
			t0 := time.Now()
			hashCount := *blockCryptoBenchmarkRepeat
			for i := 0; i < hashCount; i++ {
				blockID := h(data)
				e.Encrypt(data, blockID)
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
	blockCryptoBenchmarkCommand.Action(runBlockCryptoBenchmarkAction)
}
