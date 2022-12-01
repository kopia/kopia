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
	s, err := GetBlockSize(os.DevNull)
	require.NoError(t, err)

	if s <= 0 {
		t.Fatalf("invalid disk block size: %d, must be greater than 0", s)
	}
}

func TestGetFileAllocSize(t *testing.T) {
	const size = 4096

	d := t.TempDir()
	f := filepath.Join(d, "test")
	data := bytes.Repeat([]byte{1}, size)

	err := os.WriteFile(f, data, os.ModePerm)
	require.NoError(t, err)

	s, err := GetFileAllocSize(f)
	if err != nil {
		t.Fatalf("error getting file alloc size for %s: %v", f, err)
	}

	if s < size {
		t.Fatalf("invalid allocated file size %d, expected at least %d", s, size)
	}
}
