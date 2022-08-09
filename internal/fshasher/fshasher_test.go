package fshasher

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/testlogging"
)

//nolint:gocritic
func TestHash(t *testing.T) {
	const expectDifferentHashes = "Expected different hashes, got the same"

	root := mockfs.NewDirectory()
	root.AddFile("file1", []byte("foo-bar"), 0o444)

	d1 := root.AddDir("dir1", 0o755)
	d1.AddFile("d1-f1", []byte("d1-f1-content"), 0o644)

	ensure := require.New(t)
	ctx := testlogging.Context(t)
	h1, err := Hash(ctx, root)
	ensure.NoError(err)

	d2 := root.AddDir("dir2", 0o755)
	d2.AddFile("d1-f1", []byte("d1-f1-content"), 0o644)

	h2, err := Hash(ctx, root)
	ensure.NoError(err)
	ensure.NotEqual(h1, h2, expectDifferentHashes)

	hd1, err := Hash(ctx, d1)
	ensure.NoError(err)

	hd2, err := Hash(ctx, d2)
	ensure.NoError(err)

	ensure.Equal(hd1, hd2, "Expected same hashes, got the different ones")

	// Add an entry to d2
	d2.AddFile("f2", []byte("f2-content"), 0o444)
	hd2, err = Hash(ctx, d2)
	ensure.NoError(err)
	ensure.NotEqual(hd1, hd2, expectDifferentHashes)

	// Test different permission attributes for the top directory
	// d3 is the same as d1, but with different permissions
	d3 := root.AddDir("dir3", 0o700)
	d3.AddFile("d1-f1", []byte("d1-f1-content"), 0o644)
	hd3, err := Hash(ctx, d3)
	ensure.NoError(err)
	ensure.NotEqual(hd3, hd1, expectDifferentHashes)

	// Test different permission attributes for file
	// d4 is the same as d2, but with different permissions in d1-f1
	d4 := root.AddDir("dir4", 0o700)
	d4.AddFile("d1-f1", []byte("d1-f1-content"), 0o644)
	hd4, err := Hash(ctx, d4)
	ensure.NoError(err)
	ensure.NotEqual(hd4, hd1, expectDifferentHashes)
}
