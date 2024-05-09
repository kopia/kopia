package compression

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"sort"
	"testing"

	"github.com/kopia/kopia/internal/testutil"
)

func TestMain(m *testing.M) { testutil.MyTestMain(m) }

func TestCompressor(t *testing.T) {
	for id, comp := range ByHeaderID {
		t.Run(fmt.Sprintf("compressible-data-%x", id), func(t *testing.T) {
			// make sure all-zero data is compressed
			data := make([]byte, 10000)

			var cData bytes.Buffer

			if err := comp.Compress(&cData, bytes.NewReader(data)); err != nil {
				t.Fatalf("compression error %v", err)
				return
			}

			if cData.Len() >= len(data) {
				t.Errorf("compression not effective for all-zero data (len: %v, expected less than %v)", cData.Len(), len(data))
			}

			for id2, comp2 := range ByHeaderID {
				if id != id2 {
					var dData bytes.Buffer

					if err2 := comp2.Decompress(&dData, bytes.NewReader(cData.Bytes()), true); err2 == nil {
						t.Errorf("compressor %x was able to decompress results of %x", id2, id)
					}
				}
			}

			var data2 bytes.Buffer
			if err := comp.Decompress(&data2, bytes.NewReader(cData.Bytes()), true); err != nil {
				t.Fatalf("decompression error %v", err)
			}

			if !bytes.Equal(data, data2.Bytes()) {
				t.Errorf("invalid decompressed data %x, wanted %x", data2, data)
			}

			t.Logf("compressed %v => %v", len(data), cData.Len())
		})

		t.Run(fmt.Sprintf("non-compressible-data-%x", id), func(t *testing.T) {
			// make sure all-random data is not compressed
			data := make([]byte, 10000)
			rand.Read(data)

			var cData bytes.Buffer

			err := comp.Compress(&cData, bytes.NewReader(data))
			if err != nil {
				t.Fatalf("compression error %v", err)
				return
			}

			if cData.Len() < len(data) {
				t.Errorf("compression magically effective for random data")
			}

			var data2 bytes.Buffer
			if err := comp.Decompress(&data2, &cData, true); err != nil {
				t.Fatalf("decompression error %v", err)
			}

			if !bytes.Equal(data, data2.Bytes()) {
				t.Errorf("invalid decompressed data %x, wanted %x", data2, data)
			}

			t.Logf("compressed %v => %v", len(data), cData.Bytes())
		})
	}
}

const benchmarkDataSize = 10000000

func BenchmarkCompressor(b *testing.B) {
	compressibleData := bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, benchmarkDataSize/10)
	zeroData := make([]byte, benchmarkDataSize)

	rndData := make([]byte, benchmarkDataSize)
	rand.Read(rndData)

	var cData, dData bytes.Buffer

	var sortedNames []Name
	for id := range ByName {
		sortedNames = append(sortedNames, id)
	}

	sort.Slice(sortedNames, func(i, j int) bool {
		return sortedNames[i] < sortedNames[j]
	})

	for _, id := range sortedNames {
		comp := ByName[id]

		b.Run(fmt.Sprintf("%v-compress-zeroes", id), func(b *testing.B) {
			compressionBenchmark(b, comp, zeroData, &cData)
		})
		b.Run(fmt.Sprintf("%v-decompress-zeroes", id), func(b *testing.B) {
			decompressionBenchmark(b, comp, cData.Bytes(), &dData)
		})

		b.Run(fmt.Sprintf("%v-compress-compressible", id), func(b *testing.B) {
			compressionBenchmark(b, comp, compressibleData, &cData)
		})
		b.Run(fmt.Sprintf("%v-decompress-compressible", id), func(b *testing.B) {
			decompressionBenchmark(b, comp, cData.Bytes(), &dData)
		})

		b.Run(fmt.Sprintf("%v-compress-random", id), func(b *testing.B) {
			compressionBenchmark(b, comp, rndData, &cData)
		})
		b.Run(fmt.Sprintf("%v-decompress-random", id), func(b *testing.B) {
			decompressionBenchmark(b, comp, cData.Bytes(), &dData)
		})
	}
}

func compressionBenchmark(b *testing.B, comp Compressor, input []byte, output *bytes.Buffer) {
	b.Helper()
	b.ReportAllocs()

	rdr := bytes.NewReader(input)

	for range b.N {
		output.Reset()
		rdr.Reset(input)

		if err := comp.Compress(output, rdr); err != nil {
			b.Fatalf("compression error %v", err)
			return
		}
	}
}

func decompressionBenchmark(b *testing.B, comp Compressor, input []byte, output *bytes.Buffer) {
	b.Helper()
	b.ReportAllocs()

	rdr := bytes.NewReader(input)

	for range b.N {
		output.Reset()

		rdr.Reset(input)

		if err := comp.Decompress(output, rdr, true); err != nil {
			b.Fatalf("compression error %v", err)
			return
		}
	}
}
