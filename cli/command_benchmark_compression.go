package cli

import (
	"bytes"
	"context"
	"hash/fnv"
	"io"
	"os"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/compression"
)

const defaultCompressedDataByMethod = 128 << 20 // 128 MB

type commandBenchmarkCompression struct {
	repeat       int
	dataFile     string
	bySize       bool
	verifyStable bool
	optionPrint  bool

	out textOutput
}

func (c *commandBenchmarkCompression) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("compression", "Run compression benchmarks")
	cmd.Flag("repeat", "Number of repetitions").Default("0").IntVar(&c.repeat)
	cmd.Flag("data-file", "Use data from the given file").Required().ExistingFileVar(&c.dataFile)
	cmd.Flag("by-size", "Sort results by size").BoolVar(&c.bySize)
	cmd.Flag("verify-stable", "Verify that compression is stable").BoolVar(&c.verifyStable)
	cmd.Flag("print-options", "Print out options usable for repository creation").BoolVar(&c.optionPrint)
	cmd.Action(svc.noRepositoryAction(c.run))
	c.out.setup(svc)
}

func (c *commandBenchmarkCompression) readInputFile(ctx context.Context) ([]byte, error) {
	f, err := os.Open(c.dataFile)
	if err != nil {
		return nil, errors.Wrap(err, "error opening input file")
	}

	defer f.Close() // nolint:errcheck,gosec

	st, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat error")
	}

	dataLength := st.Size()
	if dataLength > defaultCompressedDataByMethod {
		dataLength = defaultCompressedDataByMethod

		log(ctx).Infof("NOTICE: The provided input file is too big, using first %v.", units.BytesStringBase2(dataLength))
	}

	data := make([]byte, dataLength)

	if _, err := io.ReadFull(f, data); err != nil {
		return nil, errors.Wrap(err, "error reading file")
	}

	return data, nil
}

func (c *commandBenchmarkCompression) run(ctx context.Context) error {
	type benchResult struct {
		compression    compression.Name
		throughput     float64
		compressedSize int64
	}

	var results []benchResult

	data, err := c.readInputFile(ctx)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return errors.Errorf("empty data file")
	}

	log(ctx).Infof("Compressing input file %q (%v) using all compression methods.", c.dataFile, units.BytesStringBase2(int64(len(data))))

	repeatCount := c.repeat

	if repeatCount == 0 {
		repeatCount = defaultCompressedDataByMethod / len(data)

		if repeatCount == 0 {
			repeatCount = 1
		}
	}

	log(ctx).Infof("Repeating %v times per compression method (total %v). Override with --repeat=N.", repeatCount, units.BytesStringBase2(int64(repeatCount*len(data))))

	for name, comp := range compression.ByName {
		log(ctx).Infof("Benchmarking compressor '%v'...", name)

		tt := timetrack.Start()
		cnt := repeatCount

		var (
			compressedSize int64
			lastHash       uint64
			compressed     bytes.Buffer
		)

		input := bytes.NewReader(nil)

		for i := 0; i < cnt; i++ {
			compressed.Reset()
			input.Reset(data)

			if err := comp.Compress(&compressed, input); err != nil {
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

	c.out.printStdout("     %-30v %-15v %v\n", "Compression", "Compressed Size", "Throughput")
	c.out.printStdout("-----------------------------------------------------------------\n")

	for ndx, r := range results {
		c.out.printStdout("%3d. %-30v %-15v %v / second", ndx, r.compression, r.compressedSize, units.BytesStringBase2(int64(r.throughput)))

		if c.optionPrint {
			c.out.printStdout(", --compression=%s", r.compression)
		}

		c.out.printStdout("\n")
	}

	return nil
}

func hashOf(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)

	return h.Sum64()
}
