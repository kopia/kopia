package repo_test

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/object"
)

func BenchmarkWriterDedup1M(b *testing.B) {
	ctx, env := repotesting.NewEnvironment(b, format.FormatVersion2)
	dataBuf := make([]byte, 4<<20)

	writer := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{})
	writer.Write(dataBuf)
	_, err := writer.Result()
	require.NoError(b, err)
	writer.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// write exactly the same data
		writer := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{})
		writer.Write(dataBuf)
		writer.Result()
		writer.Close()
	}
}

func BenchmarkWriterNoDedup1M(b *testing.B) {
	ctx, env := repotesting.NewEnvironment(b, format.FormatVersion2)
	dataBuf := make([]byte, 4<<20)
	chunkSize := 32
	offset := 0

	_, err := rand.Read(dataBuf)
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// write exactly the same data
		writer := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{})

		if i+chunkSize > len(dataBuf) {
			chunkSize++

			offset = 0
		}

		writer.Write(dataBuf[offset : offset+chunkSize])
		writer.Result()
		writer.Close()

		offset++
	}
}
