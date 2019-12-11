package compression

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"
)

func TestCompressor(t *testing.T) {
	for id, comp := range ByHeaderID {
		id, comp := id, comp

		t.Run(fmt.Sprintf("compressible-data-%x", id), func(t *testing.T) {
			// make sure all-zero data is compressed
			data := make([]byte, 10000)

			cData, err := comp.Compress(data)
			if err != nil {
				t.Fatalf("compression error %v", err)
				return
			}

			if len(cData) >= len(data) {
				t.Errorf("compression not effective for all-zero data")
			}

			for id2, comp2 := range ByHeaderID {
				if id != id2 {
					if _, err2 := comp2.Decompress(cData); err2 == nil {
						t.Errorf("compressor %x was able to decompress results of %x", id2, id)
					}
				}
			}

			data2, err := comp.Decompress(cData)
			if err != nil {
				t.Fatalf("decompression error %v", err)
			}

			if !bytes.Equal(data, data2) {
				t.Errorf("invalid decompressed data %x, wanted %x", data2, data)
			}

			t.Logf("compressed %v => %v", len(data), len(cData))
		})

		t.Run(fmt.Sprintf("non-compressible-data-%x", id), func(t *testing.T) {
			// make sure all-random data is not compressed
			data := make([]byte, 10000)
			rand.Read(data) //nolint:errcheck

			cData, err := comp.Compress(data)
			if err != nil {
				t.Fatalf("compression error %v", err)
				return
			}

			if len(cData) < len(data) {
				t.Errorf("compression magically effective for random data")
			}

			data2, err := comp.Decompress(cData)
			if err != nil {
				t.Fatalf("decompression error %v", err)
			}

			if !bytes.Equal(data, data2) {
				t.Errorf("invalid decompressed data %x, wanted %x", data2, data)
			}

			t.Logf("compressed %v => %v", len(data), len(cData))
		})
	}
}
