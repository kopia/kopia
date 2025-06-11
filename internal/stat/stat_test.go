//go:build !windows
// +build !windows

package stat

import (
	"bytes"
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

func TestGetFileAllocSize(t *testing.T) {
	const expectedMinAllocSize = 512

	d := t.TempDir()
	f := filepath.Join(d, "test")
	data := bytes.Repeat([]byte{1}, expectedMinAllocSize)

	err := os.WriteFile(f, data, os.ModePerm)
	require.NoError(t, err)

	s, err := GetFileAllocSize(f)

	require.NoError(t, err, "error getting file alloc size for %s: %v", f, err)
	require.GreaterOrEqual(t, s, uint64(expectedMinAllocSize), "invalid allocated file size %d, expected at least %d", s, expectedMinAllocSize)
	t.Log("file alloc size:", s)
}
