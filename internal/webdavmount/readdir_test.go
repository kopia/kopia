package webdavmount_test

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/webdav"

	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/webdavmount"
)

func openRoot(t *testing.T, d *mockfs.Directory) webdav.File {
	t.Helper()

	f, err := webdavmount.WebDAVFS(d).OpenFile(testlogging.Context(t), "/", os.O_RDONLY, 0)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, f.Close())
	})

	return f
}

func rootWithFiles(names ...string) *mockfs.Directory {
	d := mockfs.NewDirectory()

	for _, n := range names {
		d.AddFile(n, []byte("hello"), 0o644)
	}

	return d
}

func names(fis []os.FileInfo) []string {
	var result []string

	for _, fi := range fis {
		result = append(result, fi.Name())
	}

	return result
}

func Test_Readdir_pagedCallsReturnEveryEntry(t *testing.T) {
	f := openRoot(t, rootWithFiles("a", "b", "c", "d", "e"))

	const pageSize = 2

	var got []string

	// bounded so that an implementation which never signals the end of the
	// directory fails here instead of looping forever.
	for range 10 {
		fis, err := f.Readdir(pageSize)
		if err != nil {
			require.ErrorIs(t, err, io.EOF)

			break
		}

		if len(fis) == 0 {
			break
		}

		require.LessOrEqual(t, len(fis), pageSize)

		got = append(got, names(fis)...)
	}

	require.Equal(t, []string{"a", "b", "c", "d", "e"}, got)
}

func Test_Readdir_returnsEOFPastLastEntry(t *testing.T) {
	f := openRoot(t, rootWithFiles("a"))

	fis, err := f.Readdir(1)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, names(fis))

	fis, err = f.Readdir(1)
	require.ErrorIs(t, err, io.EOF)
	require.Empty(t, fis)
}

func Test_Readdir_skippedSymlinkDoesNotShortenPage(t *testing.T) {
	d := rootWithFiles("b", "c")
	d.AddSymlink("a", "b", 0o777)

	f := openRoot(t, d)

	fis, err := f.Readdir(2)
	require.NoError(t, err)
	require.Equal(t, []string{"b", "c"}, names(fis))
}

func Test_Readdir_nonPositiveCountReturnsEverythingWithNilError(t *testing.T) {
	f := openRoot(t, rootWithFiles("a", "b", "c"))

	fis, err := f.Readdir(0)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, names(fis))

	fis, err = f.Readdir(0)
	require.NoError(t, err)
	require.Empty(t, fis)
}
