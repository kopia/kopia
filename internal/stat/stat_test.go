//go:build !windows
// +build !windows

package stat

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestGetBlockSize(t *testing.T) {
	s, err := GetBlockSize(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

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
	if err != nil {
		t.Fatal(err)
	}

	s, err := GetFileAllocSize(f)
	if err != nil {
		t.Fatalf("error getting file alloc size for %s: %v", f, err)
	}

	if s < size {
		t.Fatalf("invalid allocated file size %d, expected at least %d", s, size)
	}
}
