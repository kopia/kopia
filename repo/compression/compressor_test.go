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

			var cData bytes.Buffer

			if err := comp.Compress(&cData, data); err != nil {
				t.Fatalf("compression error %v", err)
				return
			}

			if cData.Len() >= len(data) {
				t.Errorf("compression not effective for all-zero data")
			}

			for id2, comp2 := range ByHeaderID {
				if id != id2 {
					var dData bytes.Buffer

					if err2 := comp2.Decompress(&dData, cData.Bytes()); err2 == nil {
						t.Errorf("compressor %x was able to decompress results of %x", id2, id)
					}
				}
			}

			var data2 bytes.Buffer
			if err := comp.Decompress(&data2, cData.Bytes()); err != nil {
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

			err := comp.Compress(&cData, data)
			if err != nil {
				t.Fatalf("compression error %v", err)
				return
			}

			if cData.Len() < len(data) {
				t.Errorf("compression magically effective for random data")
			}

			var data2 bytes.Buffer
			if err := comp.Decompress(&data2, cData.Bytes()); err != nil {
				t.Fatalf("decompression error %v", err)
			}

			if !bytes.Equal(data, data2.Bytes()) {
				t.Errorf("invalid decompressed data %x, wanted %x", data2, data)
			}

			t.Logf("compressed %v => %v", len(data), cData.Bytes())
		})
	}
}
