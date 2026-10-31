package restore

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/mockfs"
)

type zipSizeOverrideFile struct {
	fs.File
	size int64
}

func (f zipSizeOverrideFile) Size() int64 { return f.size }

type zipMemoryWriter struct{ *bytes.Buffer }

func (w zipMemoryWriter) Close() error { return nil }

func TestZipOutputDetectsSnapshotLengthMismatch(t *testing.T) {
	cases := []struct {
		name       string
		content    string
		reported   int64
		shouldFail bool
	}{
		{name: "short clean EOF", content: "five!", reported: 10, shouldFail: true},
		{name: "overlong clean EOF", content: "five!", reported: 3, shouldFail: true},
		{name: "exact length", content: "five!", reported: 5},
		{name: "empty file", content: "", reported: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			out := NewZipOutput(zipMemoryWriter{buf}, zip.Store)
			file := zipSizeOverrideFile{
				File: mockfs.NewFile("data.txt", []byte(tc.content), 0o600),
				size: tc.reported,
			}
			err := out.WriteFile(context.Background(), "data.txt", file, nil)
			if tc.shouldFail {
				if err == nil {
					t.Fatalf("ZIP restore reported success: snapshot size=%d, actual reader size=%d", tc.reported, len(tc.content))
				}
				// A rejected restore may leave a partial output archive. Callers
				// must treat WriteFile errors as failure and discard that output.
				return
			}
			if err != nil {
				t.Fatalf("WriteFile returned error on exact-size source: %v", err)
			}
			if err := out.Close(context.Background()); err != nil {
				t.Fatalf("closing ZIP output: %v", err)
			}
			archive, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			if err != nil {
				t.Fatalf("reading completed ZIP: %v", err)
			}
			if len(archive.File) != 1 {
				t.Fatalf("expected one ZIP file, got %d", len(archive.File))
			}
			r, err := archive.File[0].Open()
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(r)
			r.Close()
			if err != nil || string(data) != tc.content {
				t.Fatalf("archive data=%q, err=%v, want %q", data, err, tc.content)
			}
		})
	}
}
