package cli

import (
	"bytes"
	"hash/fnv"
	"io/ioutil"
	"sort"
	"time"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/compression"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	benchmarkCompressionCommand      = benchmarkCommands.Command("compression", "Run compression benchmarks")
	benchmarkCompressionBlockSize    = benchmarkCompressionCommand.Flag("block-size", "Size of a block to encrypt").Default("1MB").Bytes()
	benchmarkCompressionRepeat       = benchmarkCompressionCommand.Flag("repeat", "Number of repetitions").Default("100").Int()
	benchmarkCompressionDataFile     = benchmarkCompressionCommand.Flag("data-file", "Use data from the given file instead of empty").ExistingFile()
	benchmarkCompressionBySize       = benchmarkCompressionCommand.Flag("by-size", "Sort results by size").Bool()
	benchmarkCompressionVerifyStable = benchmarkCompressionCommand.Flag("verify-stable", "Verify that compression is stable").Bool()
)

func runBenchmarkCompressionAction(ctx *kingpin.ParseContext) error {
	type benchResult struct {
		compression    compression.Name
		throughput     float64
		compressedSize int64
	}

	var results []benchResult

	data := make([]byte, *benchmarkCompressionBlockSize)

	if *benchmarkCompressionDataFile != "" {
		d, err := ioutil.ReadFile(*benchmarkCompressionDataFile)
		if err != nil {
			return err
		}

		data = d
	}

	for name, comp := range compression.ByName {
		printStderr("Benchmarking compressor '%v' (%v x %v bytes)\n", name, *benchmarkCompressionRepeat, len(data))

		t0 := time.Now()

		var compressedSize int64

		var lastHash uint64

		cnt := *benchmarkCompressionRepeat

		var compressed bytes.Buffer

		for i := 0; i < cnt; i++ {
			compressed.Reset()

			if err := comp.Compress(&compressed, data); err != nil {
				printStderr("compression %q failed: %v\n", name, err)
				continue
			}

			compressedSize = int64(compressed.Len())

			if *benchmarkCompressionVerifyStable {
				h := hashOf(compressed.Bytes())

				if i == 0 {
					lastHash = h
				} else if h != lastHash {
					printStderr("compression %q is not stable\n", name)
					continue
				}
			}
		}

		hashTime := time.Since(t0)
		bytesPerSecond := float64(len(data)) * float64(cnt) / hashTime.Seconds()

		results = append(results, benchResult{compression: name, throughput: bytesPerSecond, compressedSize: compressedSize})
	}

	if *benchmarkCompressionBySize {
		sort.Slice(results, func(i, j int) bool {
			return results[i].compressedSize < results[j].compressedSize
		})
	} else {
		sort.Slice(results, func(i, j int) bool {
			return results[i].throughput > results[j].throughput
		})
	}

	printStdout("     %-30v %-15v %v\n", "Compression", "Compressed Size", "Throughput")
	printStdout("-----------------------------------------------------------------\n")

	for ndx, r := range results {
		printStdout("%3d. %-30v %-15v %v / second\n", ndx, r.compression, r.compressedSize, units.BytesStringBase2(int64(r.throughput)))
	}

	return nil
}

func init() {
	benchmarkCompressionCommand.Action(runBenchmarkCompressionAction)
}

func hashOf(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b) //nolint:errcheck

	return h.Sum64()
}
