package hashing_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/hashing"
)

type parameters struct {
	hashingAlgo string
	hmacSecret  []byte
}

func (p parameters) GetHashFunction() string { return p.hashingAlgo }
func (p parameters) GetHmacSecret() []byte   { return p.hmacSecret }

func TestRoundTrip(t *testing.T) {
	data1 := make([]byte, 100)
	rand.Read(data1)

	data2 := make([]byte, 100)
	rand.Read(data2)

	hmacSecret := make([]byte, 32)
	rand.Read(hmacSecret)

	for _, hashingAlgo := range hashing.SupportedAlgorithms() {
		t.Run(hashingAlgo, func(t *testing.T) {
			f, err := hashing.CreateHashFunc(parameters{hashingAlgo, hmacSecret})
			if err != nil {
				t.Fatal(err)
			}

			outputBuffer := make([]byte, 0, 256)
			hash1a := f(nil, gather.FromSlice(data1))
			hash1b := f(outputBuffer, gather.FromSlice(data1))
			hash2 := f(nil, gather.FromSlice(data2))

			if !bytes.Equal(hash1a, hash1b) {
				t.Fatalf("hashing not stable: %x %x", hash1a, hash1b)
			}

			if !bytes.Equal(hash1a, outputBuffer[0:len(hash1a)]) {
				t.Fatalf("hash did not populate output buffer")
			}

			if bytes.Equal(hash1a, hash2) {
				t.Fatalf("hashing should produce different results: %x", hash1a)
			}
		})
	}
}
