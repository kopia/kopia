//go:build !windows
// +build !windows

package stat

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetBlockSize(t *testing.T) {
	size, err := GetBlockSize(os.DevNull)
	require.NoError(t, err)

	require.Positive(t, size)
}

func TestGetBlockSizeFromCurrentFS(t *testing.T) {
	size, err := GetBlockSize(".")

	require.NoError(t, err)
	require.Positive(t, size)
}

func TestGetFileAllocSize(t *testing.T) {
	const expectedMinAllocSize = 512 // minimum block size on most platforms

	d := t.TempDir()
	f := filepath.Join(d, "test")

	err := os.WriteFile(f, []byte{1}, os.ModePerm)
	require.NoError(t, err)

	s, err := GetFileAllocSize(f)

	require.NoError(t, err, "error getting file alloc size for %s: %v", f, err)
	t.Log("file alloc size:", s)
	require.GreaterOrEqual(t, s, uint64(expectedMinAllocSize), "invalid allocated file size %d, expected at least %d", s, expectedMinAllocSize)
	require.Zerof(t, s%expectedMinAllocSize, "file allocated size is expected to be a multiple of %v", expectedMinAllocSize)
}
