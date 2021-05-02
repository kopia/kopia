package cli

import (
	"bytes"
	"context"
	"hash/fnv"
	"io/ioutil"
	"sort"

	atunits "github.com/alecthomas/units"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/compression"
)

type commandBenchmarkCompression struct {
	blockSize    atunits.Base2Bytes
	repeat       int
	dataFile     string
	bySize       bool
	verifyStable bool
	optionPrint  bool
}

func (c *commandBenchmarkCompression) setup(parent commandParent) {
	cmd := parent.Command("compression", "Run compression benchmarks")
	cmd.Flag("block-size", "Size of a block to compress").Default("1MB").BytesVar(&c.blockSize)
	cmd.Flag("repeat", "Number of repetitions").Default("100").IntVar(&c.repeat)
	cmd.Flag("data-file", "Use data from the given file instead of empty").ExistingFileVar(&c.dataFile)
	cmd.Flag("by-size", "Sort results by size").BoolVar(&c.bySize)
	cmd.Flag("verify-stable", "Verify that compression is stable").BoolVar(&c.verifyStable)
	cmd.Flag("print-options", "Print out options usable for repository creation").BoolVar(&c.optionPrint)
	cmd.Action(noRepositoryAction(c.run))
}

func (c *commandBenchmarkCompression) run(ctx context.Context) error {
	type benchResult struct {
		compression    compression.Name
		throughput     float64
		compressedSize int64
	}

	var results []benchResult

	data := make([]byte, c.blockSize)

	if c.dataFile != "" {
		d, err := ioutil.ReadFile(c.dataFile)
		if err != nil {
			return errors.Wrap(err, "error reading compression data file")
		}

		data = d
	}

	for name, comp := range compression.ByName {
		log(ctx).Infof("Benchmarking compressor '%v' (%v x %v bytes)", name, c.repeat, len(data))

		tt := timetrack.Start()

		var compressedSize int64

		var lastHash uint64

		cnt := c.repeat

		var compressed bytes.Buffer

		for i := 0; i < cnt; i++ {
			compressed.Reset()

			if err := comp.Compress(&compressed, data); err != nil {
				log(ctx).Errorf("compression %q failed: %v", name, err)
				continue
			}

			compressedSize = int64(compressed.Len())

			if c.verifyStable {
				h := hashOf(compressed.Bytes())

				if i == 0 {
					lastHash = h
				} else if h != lastHash {
					log(ctx).Errorf("compression %q is not stable", name)
					continue
				}
			}
		}

		_, perSecond := tt.Completed(float64(len(data)) * float64(cnt))

		results = append(results, benchResult{compression: name, throughput: perSecond, compressedSize: compressedSize})
	}

	if c.bySize {
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
		printStdout("%3d. %-30v %-15v %v / second", ndx, r.compression, r.compressedSize, units.BytesStringBase2(int64(r.throughput)))

		if c.optionPrint {
			printStdout(", --compression=%s", r.compression)
		}

		printStdout("\n")
	}

	return nil
}

func hashOf(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b) //nolint:errcheck

	return h.Sum64()
}
