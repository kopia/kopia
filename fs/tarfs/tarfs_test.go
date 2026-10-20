package tarfs_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/tarfs"
)

type tarEntry struct {
	hdr  tar.Header
	body string
}

func buildTar(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()

	var buf bytes.Buffer

	tw := tar.NewWriter(&buf)

	for i := range entries {
		e := &entries[i]
		hdr := e.hdr

		if hdr.Mode == 0 && hdr.Typeflag != tar.TypeXGlobalHeader {
			hdr.Mode = 0o644
		}

		if hdr.Typeflag == 0 {
			hdr.Typeflag = tar.TypeReg
		}

		hdr.Size = int64(len(e.body))
		require.NoError(t, tw.WriteHeader(&hdr))
		_, err := io.WriteString(tw, e.body)
		require.NoError(t, err)
	}

	require.NoError(t, tw.Close())

	return buf.Bytes()
}

func asDir(t *testing.T, e fs.Entry) fs.Directory {
	t.Helper()

	d, ok := e.(fs.Directory)
	require.True(t, ok, "%v is not a directory", e.Name())

	return d
}

func names(t *testing.T, d fs.Directory) []string {
	t.Helper()

	entries, err := fs.GetAllEntries(context.Background(), d)
	require.NoError(t, err)

	var result []string
	for _, e := range entries {
		result = append(result, e.Name())
	}

	return result
}

func readFile(t *testing.T, e fs.Entry) string {
	t.Helper()

	f, ok := e.(fs.File)
	require.True(t, ok, "%v is not a file", e.Name())

	r, err := f.Open(context.Background())
	require.NoError(t, err)

	defer r.Close()

	data, err := io.ReadAll(r)
	require.NoError(t, err)

	return string(data)
}

func TestTarFS(t *testing.T) {
	modTime := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)

	data := buildTar(t,
		tarEntry{hdr: tar.Header{Name: "b.txt", ModTime: modTime, Uid: 12, Gid: 34}, body: "bee"},
		tarEntry{hdr: tar.Header{Name: "dir/", Typeflag: tar.TypeDir, Mode: 0o755}},
		tarEntry{hdr: tar.Header{Name: "dir/nested/deep.txt"}, body: "deep"}, // dir/nested has no header
		tarEntry{hdr: tar.Header{Name: "dir/a.txt"}, body: "aaa"},
		tarEntry{hdr: tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "b.txt", Mode: 0o777}},
		tarEntry{hdr: tar.Header{Name: "./dotted.txt"}, body: "dot"},
	)

	root, err := tarfs.NewDirectory(context.Background(), "root", bytes.NewReader(data))
	require.NoError(t, err)

	defer root.Close()

	require.Equal(t, "root", root.Name())
	require.True(t, root.IsDir())
	require.True(t, root.SupportsMultipleIterations())

	// entries come back sorted by name, twice.
	require.Equal(t, []string{"b.txt", "dir", "dotted.txt", "link"}, names(t, root))
	require.Equal(t, []string{"b.txt", "dir", "dotted.txt", "link"}, names(t, root))

	b, err := root.Child(context.Background(), "b.txt")
	require.NoError(t, err)
	require.Equal(t, "bee", readFile(t, b))
	require.Equal(t, int64(3), b.Size())
	require.True(t, modTime.Equal(b.ModTime()), "modtime %v", b.ModTime())
	require.Equal(t, os.FileMode(0o644), b.Mode())
	require.Equal(t, fs.OwnerInfo{UserID: 12, GroupID: 34}, b.Owner())
	require.Empty(t, b.LocalFilesystemPath())

	dir, err := root.Child(context.Background(), "dir")
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755)|os.ModeDir, dir.Mode())
	require.Equal(t, []string{"a.txt", "nested"}, names(t, asDir(t, dir)))

	nested, err := asDir(t, dir).Child(context.Background(), "nested")
	require.NoError(t, err)
	require.True(t, nested.IsDir(), "missing parent directories are synthesized")

	deep, err := asDir(t, nested).Child(context.Background(), "deep.txt")
	require.NoError(t, err)
	require.Equal(t, "deep", readFile(t, deep))

	_, err = root.Child(context.Background(), "missing")
	require.ErrorIs(t, err, fs.ErrEntryNotFound)

	link, err := root.Child(context.Background(), "link")
	require.NoError(t, err)

	sl, ok := link.(fs.Symlink)
	require.True(t, ok)

	target, err := sl.Readlink(context.Background())
	require.NoError(t, err)
	require.Equal(t, "b.txt", target)

	resolved, err := sl.Resolve(context.Background())
	require.NoError(t, err)
	require.Equal(t, "bee", readFile(t, resolved))
}

func TestTarFS_SeekAndConcurrentReads(t *testing.T) {
	body := make([]byte, 100_000)
	for i := range body {
		body[i] = byte(i % 251)
	}

	data := buildTar(t, tarEntry{hdr: tar.Header{Name: "big"}, body: string(body)})

	root, err := tarfs.NewDirectory(context.Background(), "root", bytes.NewReader(data))
	require.NoError(t, err)

	defer root.Close()

	e, err := root.Child(context.Background(), "big")
	require.NoError(t, err)

	f, ok := e.(fs.File)
	require.True(t, ok)

	// two readers open at once, each seeking into a different part of the file,
	// which is what the uploader does for parallel chunked uploads.
	r1, err := f.Open(context.Background())
	require.NoError(t, err)

	defer r1.Close()

	r2, err := f.Open(context.Background())
	require.NoError(t, err)

	defer r2.Close()

	_, err = r2.Seek(50_000, io.SeekStart)
	require.NoError(t, err)

	got1, err := io.ReadAll(io.LimitReader(r1, 50_000))
	require.NoError(t, err)

	got2, err := io.ReadAll(r2)
	require.NoError(t, err)

	require.Equal(t, body[:50_000], got1)
	require.Equal(t, body[50_000:], got2)

	entry, err := r1.Entry()
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), entry.Size())
}

func TestTarFS_Gzip(t *testing.T) {
	data := buildTar(t, tarEntry{hdr: tar.Header{Name: "x/y.txt"}, body: "zipped"})

	var gz bytes.Buffer

	w := gzip.NewWriter(&gz)
	_, err := w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	root, err := tarfs.NewDirectory(context.Background(), "root", bytes.NewReader(gz.Bytes()))
	require.NoError(t, err)

	defer root.Close()

	x, err := root.Child(context.Background(), "x")
	require.NoError(t, err)

	y, err := asDir(t, x).Child(context.Background(), "y.txt")
	require.NoError(t, err)
	require.Equal(t, "zipped", readFile(t, y))
}

func TestTarFS_UnsupportedEntryIsErrorEntry(t *testing.T) {
	data := buildTar(t,
		tarEntry{hdr: tar.Header{Name: "fifo", Typeflag: tar.TypeFifo}},
		tarEntry{hdr: tar.Header{Name: "ok.txt"}, body: "ok"},
	)

	root, err := tarfs.NewDirectory(context.Background(), "root", bytes.NewReader(data))
	require.NoError(t, err)

	defer root.Close()

	entries, err := fs.GetAllEntries(context.Background(), root)
	require.NoError(t, err)

	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	require.Len(t, entries, 2)

	ee, ok := entries[0].(fs.ErrorEntry)
	require.True(t, ok, "fifo should be reported as an error entry")
	require.ErrorIs(t, ee.ErrorInfo(), fs.ErrUnknown)
}

func TestTarFS_NotATar(t *testing.T) {
	_, err := tarfs.NewDirectory(context.Background(), "root", bytes.NewReader([]byte("definitely not a tar archive at all")))
	require.Error(t, err)
}

func TestTarFS_GlobalHeaderIsNotAnEntry(t *testing.T) {
	data := buildTar(t,
		tarEntry{hdr: tar.Header{Name: "pax_global_header", Typeflag: tar.TypeXGlobalHeader, PAXRecords: map[string]string{"comment": "abc"}}},
		tarEntry{hdr: tar.Header{Name: "ok.txt"}, body: "ok"},
	)

	root, err := tarfs.NewDirectory(context.Background(), "root", bytes.NewReader(data))
	require.NoError(t, err)

	defer root.Close()

	require.Equal(t, []string{"ok.txt"}, names(t, root))
}

func TestTarFS_OldGNUSparseIsRejected(t *testing.T) {
	data := buildTar(t, tarEntry{hdr: tar.Header{Name: "sparse", Typeflag: tar.TypeGNUSparse, Format: tar.FormatGNU}})

	_, err := tarfs.NewDirectory(context.Background(), "root", bytes.NewReader(data))
	require.Error(t, err)
	require.Contains(t, err.Error(), "sparse")
}

func TestTarFS_HardLinks(t *testing.T) {
	data := buildTar(t,
		tarEntry{hdr: tar.Header{Name: "orig"}, body: "shared"},
		tarEntry{hdr: tar.Header{Name: "same", Typeflag: tar.TypeLink, Linkname: "orig"}},
		tarEntry{hdr: tar.Header{Name: "dangling", Typeflag: tar.TypeLink, Linkname: "elsewhere/missing"}},
	)

	root, err := tarfs.NewDirectory(context.Background(), "root", bytes.NewReader(data))
	require.NoError(t, err)

	defer root.Close()

	same, err := root.Child(context.Background(), "same")
	require.NoError(t, err)
	require.Equal(t, "shared", readFile(t, same))
	require.Equal(t, int64(6), same.Size())

	dangling, err := root.Child(context.Background(), "dangling")
	require.NoError(t, err)

	ee, ok := dangling.(fs.ErrorEntry)
	require.True(t, ok, "a dangling hard link is reported on that entry only")
	require.ErrorIs(t, ee.ErrorInfo(), fs.ErrEntryNotFound)
	require.NotErrorIs(t, ee.ErrorInfo(), fs.ErrUnknown)
}

func TestTarFS_PathConflictsAreErrors(t *testing.T) {
	fileThenDir := buildTar(t,
		tarEntry{hdr: tar.Header{Name: "x"}, body: "file"},
		tarEntry{hdr: tar.Header{Name: "x/y.txt"}, body: "under a file"},
	)

	_, err := tarfs.NewDirectory(context.Background(), "root", bytes.NewReader(fileThenDir))
	require.ErrorContains(t, err, "x: archive entry conflicts")

	dirThenFile := buildTar(t,
		tarEntry{hdr: tar.Header{Name: "d/y.txt"}, body: "in dir"},
		tarEntry{hdr: tar.Header{Name: "d"}, body: "now a file"},
	)

	_, err = tarfs.NewDirectory(context.Background(), "root", bytes.NewReader(dirThenFile))
	require.Error(t, err)
}

func TestTarFS_UnsafeNamesAreStoredUnderRoot(t *testing.T) {
	data := buildTar(t,
		tarEntry{hdr: tar.Header{Name: "/abs.txt"}, body: "abs"},
		tarEntry{hdr: tar.Header{Name: "../../escape.txt"}, body: "esc"},
	)

	root, err := tarfs.NewDirectory(context.Background(), "root", bytes.NewReader(data))
	require.NoError(t, err)

	defer root.Close()

	require.Equal(t, []string{"abs.txt", "escape.txt"}, names(t, root))
}
