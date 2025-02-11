package cli

import (
	"bytes"
	"context"
	"hash/fnv"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/compression"
)

const defaultCompressedDataByMethod = 128 << 20 // 128 MB

type commandBenchmarkCompression struct {
	repeat       int
	dataFile     string
	bySize       bool
	byAllocated  bool
	verifyStable bool
	optionPrint  bool
	parallel     int
	deprecated   bool
	operations   string
	algorithms   string

	out textOutput
}

func (c *commandBenchmarkCompression) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("compression", "Run compression benchmarks")
	cmd.Flag("repeat", "Number of repetitions").Default("0").IntVar(&c.repeat)
	cmd.Flag("data-file", "Use data from the given file").Required().ExistingFileVar(&c.dataFile)
	cmd.Flag("by-size", "Sort results by size").BoolVar(&c.bySize)
	cmd.Flag("by-alloc", "Sort results by allocated bytes").BoolVar(&c.byAllocated)
	cmd.Flag("parallel", "Number of parallel goroutines").Default("1").IntVar(&c.parallel)
	cmd.Flag("operations", "Operations").Default("both").EnumVar(&c.operations, "compress", "decompress", "both")
	cmd.Flag("verify-stable", "Verify that compression is stable").BoolVar(&c.verifyStable)
	cmd.Flag("print-options", "Print out options usable for repository creation").BoolVar(&c.optionPrint)
	cmd.Flag("deprecated", "Included deprecated compression algorithms").BoolVar(&c.deprecated)
	cmd.Flag("algorithms", "Comma-separated list of algorithms to benchmark").StringVar(&c.algorithms)
	cmd.Action(svc.noRepositoryAction(c.run))
	c.out.setup(svc)
}

func (c *commandBenchmarkCompression) readInputFile(ctx context.Context) ([]byte, error) {
	f, err := os.Open(c.dataFile)
	if err != nil {
		return nil, errors.Wrap(err, "error opening input file")
	}

	defer f.Close() //nolint:errcheck

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

type compressionBechmarkResult struct {
	compression    compression.Name
	throughput     float64
	compressedSize uint64
	allocations    uint64
	allocBytes     uint64
}

func (c *commandBenchmarkCompression) shouldIncludeAlgorithm(name compression.Name) bool {
	if c.algorithms == "" {
		if compression.IsDeprecated[name] && !c.deprecated {
			return false
		}

		return true
	}

	for _, a := range strings.Split(c.algorithms, ",") {
		if strings.HasPrefix(string(name), a) {
			return true
		}
	}

	return false
}

func (c *commandBenchmarkCompression) run(ctx context.Context) error {
	var benchmarkCompression, benchmarkDecompression bool

	data, err := c.readInputFile(ctx)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return errors.New("empty data file")
	}

	repeatCount := c.repeat

	if repeatCount == 0 {
		repeatCount = defaultCompressedDataByMethod / len(data)

		if repeatCount == 0 {
			repeatCount = 1
		}
	}

	algorithms := map[compression.Name]compression.Compressor{}

	for name, comp := range compression.ByName {
		if c.shouldIncludeAlgorithm(name) {
			algorithms[name] = comp
		}
	}

	log(ctx).Infof("Will repeat each benchmark %v times per compression method (total %v). Override with --repeat=N.", repeatCount, units.BytesString(repeatCount*len(data)))

	switch c.operations {
	case "compress":
		benchmarkCompression = true
		benchmarkDecompression = false
	case "decompress":
		benchmarkCompression = false
		benchmarkDecompression = true
	default:
		benchmarkCompression = true
		benchmarkDecompression = true
	}

	if benchmarkCompression {
		if err := c.runCompression(ctx, data, repeatCount, algorithms); err != nil {
			return err
		}
	}

	if benchmarkDecompression {
		if err := c.runDecompression(ctx, data, repeatCount, algorithms); err != nil {
			return err
		}
	}

	return nil
}

func (c *commandBenchmarkCompression) runCompression(ctx context.Context, data []byte, repeatCount int, algorithms map[compression.Name]compression.Compressor) error {
	var results []compressionBechmarkResult

	log(ctx).Infof("Compressing input file %q (%v) using %v compression methods.", c.dataFile, units.BytesString(len(data)), len(algorithms))

	for name, comp := range algorithms {
		log(ctx).Infof("Benchmarking compressor '%v'...", name)

		cnt := repeatCount

		runtime.GC()

		var startMS, endMS runtime.MemStats

		run := func(compressed *bytes.Buffer) uint64 {
			var (
				compressedSize uint64
				lastHash       uint64
				input          = bytes.NewReader(nil)
			)

			for i := range cnt {
				compressed.Reset()
				input.Reset(data)

				if err := comp.Compress(compressed, input); err != nil {
					log(ctx).Errorf("compression %q failed: %v", name, err)
					continue
				}

				compressedSize = uint64(compressed.Len()) //nolint:gosec

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

			return compressedSize
		}

		outputBuffers := makeOutputBuffers(c.parallel, defaultCompressedDataByMethod)

		tt := timetrack.Start()

		runtime.ReadMemStats(&startMS)

		compressedSize := runInParallel(outputBuffers, run)

		runtime.ReadMemStats(&endMS)

		_, perSecond := tt.Completed(float64(c.parallel) * float64(len(data)) * float64(cnt))

		results = append(results,
			compressionBechmarkResult{
				compression:    name,
				throughput:     perSecond,
				compressedSize: compressedSize,
				allocations:    endMS.Mallocs - startMS.Mallocs,
				allocBytes:     endMS.TotalAlloc - startMS.TotalAlloc,
			})
	}

	c.sortResults(results)
	c.printResults(results)

	return nil
}

func (c *commandBenchmarkCompression) runDecompression(ctx context.Context, data []byte, repeatCount int, algorithms map[compression.Name]compression.Compressor) error {
	var results []compressionBechmarkResult

	log(ctx).Infof("Decompressing input file %q (%v) using %v compression methods.", c.dataFile, units.BytesString(len(data)), len(algorithms))

	var compressedInput gather.WriteBuffer
	defer compressedInput.Close()

	for name, comp := range algorithms {
		compressedInput.Reset()

		if err := comp.Compress(&compressedInput, bytes.NewReader(data)); err != nil {
			return errors.Wrapf(err, "unable to compress data using %v", name)
		}

		compressedInputBytes := compressedInput.ToByteSlice()

		log(ctx).Infof("Benchmarking decompressor '%v'...", name)

		cnt := repeatCount

		runtime.GC()

		var startMS, endMS runtime.MemStats

		run := func(decompressed *bytes.Buffer) uint64 {
			input := bytes.NewReader(nil)

			for range cnt {
				decompressed.Reset()
				input.Reset(compressedInputBytes)

				if err := comp.Decompress(decompressed, input, true); err != nil {
					log(ctx).Errorf("decompression %q failed: %v", name, err)
				}
			}

			//nolint:gosec
			return uint64(compressedInput.Length())
		}

		outputBuffers := makeOutputBuffers(c.parallel, defaultCompressedDataByMethod)

		tt := timetrack.Start()

		runtime.ReadMemStats(&startMS)

		compressedSize := runInParallel(outputBuffers, run)

		runtime.ReadMemStats(&endMS)

		_, perSecond := tt.Completed(float64(c.parallel) * float64(len(data)) * float64(cnt))

		results = append(results,
			compressionBechmarkResult{
				compression:    name,
				throughput:     perSecond,
				compressedSize: compressedSize,
				allocations:    endMS.Mallocs - startMS.Mallocs,
				allocBytes:     endMS.TotalAlloc - startMS.TotalAlloc,
			})
	}

	c.sortResults(results)
	c.printResults(results)

	return nil
}

func (c *commandBenchmarkCompression) sortResults(results []compressionBechmarkResult) {
	switch {
	case c.bySize:
		sort.Slice(results, func(i, j int) bool {
			return results[i].compressedSize < results[j].compressedSize
		})
	case c.byAllocated:
		sort.Slice(results, func(i, j int) bool {
			return results[i].allocBytes < results[j].allocBytes
		})
	default:
		sort.Slice(results, func(i, j int) bool {
			return results[i].throughput > results[j].throughput
		})
	}
}

func (c *commandBenchmarkCompression) printResults(results []compressionBechmarkResult) {
	c.out.printStdout("     %-26v %-12v %-12v %v\n", "Compression", "Compressed", "Throughput", "Allocs   Memory Usage")
	c.out.printStdout("------------------------------------------------------------------------------------------------\n")

	for ndx, r := range results {
		maybeDeprecated := ""
		if compression.IsDeprecated[r.compression] {
			maybeDeprecated = " (deprecated)"
		}

		c.out.printStdout("%3d. %-26v %-12v %8v/s     %-8v %v%v",
			ndx,
			r.compression,
			units.BytesString(r.compressedSize),
			units.BytesString(r.throughput),
			r.allocations,
			units.BytesString(r.allocBytes),
			maybeDeprecated,
		)

		if c.optionPrint {
			c.out.printStdout(", --compression=%s", r.compression)
		}

		c.out.printStdout("\n")
	}
}

func hashOf(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)

	return h.Sum64()
}
