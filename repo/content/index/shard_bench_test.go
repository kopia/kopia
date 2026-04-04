package index

import (
	"hash/fnv"
	"io"
	"testing"
)

func BenchmarkShard_Append(b *testing.B) {
	id, err := ParseID("xabcdef0123456789abcdef0123456789")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		h := fnv.New32a()

		var buf [64]byte

		h.Write(id.Append(buf[:0])) //nolint:errcheck

		_ = h.Sum32()
	}
}

func BenchmarkShard_String(b *testing.B) {
	id, err := ParseID("xabcdef0123456789abcdef0123456789")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		h := fnv.New32a()
		io.WriteString(h, id.String()) //nolint:errcheck

		_ = h.Sum32()
	}
}
