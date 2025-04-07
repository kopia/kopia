package localfs_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
)

func BenchmarkReadDir0(b *testing.B) {
	benchmarkReadDirWithCount(b, 0)
}

func BenchmarkReadDir1(b *testing.B) {
	benchmarkReadDirWithCount(b, 1)
}

func BenchmarkReadDir2(b *testing.B) {
	benchmarkReadDirWithCount(b, 2)
}

func BenchmarkReadDir10(b *testing.B) {
	benchmarkReadDirWithCount(b, 10)
}

func BenchmarkReadDir100(b *testing.B) {
	benchmarkReadDirWithCount(b, 100)
}

func BenchmarkReadDir1000(b *testing.B) {
	benchmarkReadDirWithCount(b, 1000)
}

func BenchmarkReadDir10000(b *testing.B) {
	benchmarkReadDirWithCount(b, 10000)
}

func benchmarkReadDirWithCount(b *testing.B, fileCount int) {
	b.Helper()

	b.StopTimer()

	td := b.TempDir()

	for range fileCount {
		os.WriteFile(filepath.Join(td, uuid.NewString()), []byte{1, 2, 3, 4}, 0o644)
	}

	b.StartTimer()

	ctx := context.Background()

	for range b.N {
		dir, _ := localfs.Directory(td)
		fs.IterateEntries(ctx, dir, func(context.Context, fs.Entry) error {
			return nil
		})
	}
}
