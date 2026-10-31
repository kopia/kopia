package restore

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/kopia/kopia/fs"
)

type zipEntryTestBuffer struct{ bytes.Buffer }

func (b *zipEntryTestBuffer) Close() error { return nil }

// These lightweight entries delegate unused fs.Entry methods to the embedded
// interface. ZIP output uses only Mode, ModTime, and Readlink for these types.
type zipEntryTestDirectory struct{ fs.Directory }

func (zipEntryTestDirectory) Mode() os.FileMode { return os.ModeDir | 0750 }
func (zipEntryTestDirectory) ModTime() time.Time {
	return time.Date(2020, time.May, 3, 4, 5, 6, 0, time.UTC)
}

type zipEntryTestSymlink struct {
	fs.Symlink
	target string
	err    error
}

func (zipEntryTestSymlink) Mode() os.FileMode { return os.ModeSymlink | 0777 }
func (zipEntryTestSymlink) ModTime() time.Time {
	return time.Date(2020, time.June, 4, 5, 6, 7, 0, time.UTC)
}
func (l zipEntryTestSymlink) Readlink(context.Context) (string, error) {
	return l.target, l.err
}

type zipEntryTestReader struct{ *bytes.Reader }

func (*zipEntryTestReader) Close() error             { return nil }
func (*zipEntryTestReader) Entry() (fs.Entry, error) { return nil, nil }

type zipEntryTestFile struct {
	fs.File
	data []byte
}

func (f zipEntryTestFile) Mode() os.FileMode { return 0644 }
func (f zipEntryTestFile) ModTime() time.Time {
	return time.Date(2020, time.July, 5, 6, 7, 8, 0, time.UTC)
}
func (f zipEntryTestFile) Size() int64 { return int64(len(f.data)) }
func (f zipEntryTestFile) Open(context.Context) (fs.Reader, error) {
	return &zipEntryTestReader{bytes.NewReader(f.data)}, nil
}

func zipEntryTestOpen(t *testing.T, b *zipEntryTestBuffer) *zip.Reader {
	t.Helper()
	reader, err := zip.NewReader(bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func TestZipOutputPreservesEmptyDirectory(t *testing.T) {
	ctx := context.Background()
	var output zipEntryTestBuffer
	writer := NewZipOutput(&output, zip.Deflate)
	dir := zipEntryTestDirectory{}

	if err := writer.BeginDirectory(ctx, "", dir); err != nil {
		t.Fatal(err)
	}
	if err := writer.BeginDirectory(ctx, "empty", dir); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}

	entries := zipEntryTestOpen(t, &output).File
	if len(entries) != 1 {
		t.Fatalf("ZIP entries = %d, want exactly one empty directory", len(entries))
	}
	if entries[0].Name != "empty/" || !entries[0].Mode().IsDir() {
		t.Fatalf("ZIP directory entry = %q with mode %v", entries[0].Name, entries[0].Mode())
	}
	if entries[0].Method != zip.Store {
		t.Fatalf("ZIP directory should not be compressed, method = %d", entries[0].Method)
	}
}

func TestZipOutputPreservesSymlinkTargetWithoutFollowing(t *testing.T) {
	ctx := context.Background()
	var output zipEntryTestBuffer
	writer := NewZipOutput(&output, zip.Deflate)
	target := "../parent/file.txt"
	if err := writer.CreateSymlink(ctx, "linked", zipEntryTestSymlink{target: target}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}

	files := zipEntryTestOpen(t, &output).File
	if len(files) != 1 {
		t.Fatalf("ZIP entries = %d, want one symlink", len(files))
	}
	if files[0].Name != "linked" || files[0].Mode()&os.ModeSymlink == 0 {
		t.Fatalf("ZIP link metadata = %q, mode %v", files[0].Name, files[0].Mode())
	}
	r, err := files[0].Open()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != target {
		t.Fatalf("ZIP link target = %q, want %q", string(got), target)
	}
}

func TestZipOutputReturnsSymlinkReadError(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("failed to read symlink target")
	var output zipEntryTestBuffer
	writer := NewZipOutput(&output, zip.Store)
	if err := writer.CreateSymlink(ctx, "unreadable", zipEntryTestSymlink{err: failure}); !errors.Is(err, failure) {
		t.Fatalf("CreateSymlink error = %v, want %v", err, failure)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(zipEntryTestOpen(t, &output).File); got != 0 {
		t.Fatalf("ZIP entries = %d, want zero after failed symlink read", got)
	}
}

func TestZipOutputDirectoryLinkAndRegularFileCoexist(t *testing.T) {
	ctx := context.Background()
	var output zipEntryTestBuffer
	writer := NewZipOutput(&output, zip.Deflate)
	if err := writer.BeginDirectory(ctx, "empty", zipEntryTestDirectory{}); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteFile(ctx, "file.txt", zipEntryTestFile{data: []byte("content")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := writer.CreateSymlink(ctx, "link", zipEntryTestSymlink{target: "file.txt"}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}

	files := zipEntryTestOpen(t, &output).File
	if len(files) != 3 {
		t.Fatalf("ZIP entries = %d, want directory + regular file + symlink", len(files))
	}
	if files[0].Name != "empty/" || !files[0].Mode().IsDir() {
		t.Fatalf("first entry = %q mode %v", files[0].Name, files[0].Mode())
	}
	if files[1].Name != "file.txt" || files[1].Mode()&os.ModeType != 0 {
		t.Fatalf("regular file = %q mode %v", files[1].Name, files[1].Mode())
	}
	r, err := files[1].Open()
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || string(got) != "content" {
		t.Fatalf("regular file data = %q err = %v", got, err)
	}
	if files[2].Name != "link" || files[2].Mode()&os.ModeSymlink == 0 {
		t.Fatalf("third entry = %q mode %v", files[2].Name, files[2].Mode())
	}
}
