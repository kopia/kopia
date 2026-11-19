package snapshotfs_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

func TestNameSearcher(t *testing.T) {
	t.Parallel()

	root := mockfs.NewDirectory()
	root.AddDir("Reports", 0o755).AddFile("REPORT.txt", []byte("contents"), 0o600)
	root.AddFileWithSource("report.dat", 0o600, func() (mockfs.ReaderSeekerCloser, error) {
		t.Fatal("search must not open file contents")
		return nil, errors.New("unexpected file read")
	})
	root.AddSymlink("report.link", "/must/not/be/followed", 0o777)

	searcher, err := snapshotfs.NewNameSearcher("report", true)
	require.NoError(t, err)

	var paths []string
	require.NoError(t, searcher.Search(context.Background(), root, func(match snapshotfs.NameMatch) error {
		paths = append(paths, match.Path)
		return nil
	}))
	require.Equal(t, []string{"Reports", "Reports/REPORT.txt", "report.dat", "report.link"}, paths)

	searcher, err = snapshotfs.NewNameSearcher("REPORT", false)
	require.NoError(t, err)

	paths = nil
	require.NoError(t, searcher.Search(context.Background(), root, func(match snapshotfs.NameMatch) error {
		paths = append(paths, match.Path)
		return nil
	}))
	require.Equal(t, []string{"Reports/REPORT.txt"}, paths)

	_, err = snapshotfs.NewNameSearcher("", false)
	require.Error(t, err)
}

type countedSearchDir struct {
	fs.Directory
	reads map[string]int
}

func (d countedSearchDir) ObjectID() object.ID {
	name := d.Name()
	if name == "one" || name == "two" {
		name = "shared"
	}

	digest := sha256.Sum256([]byte(name))

	id, err := object.ParseID("k" + hex.EncodeToString(digest[:]))
	if err != nil {
		panic(err)
	}

	return id
}

func (d countedSearchDir) Iterate(ctx context.Context) (fs.DirectoryIterator, error) {
	d.reads[d.Name()]++

	iter, err := d.Directory.Iterate(ctx)
	if err != nil {
		return nil, err
	}

	return countedSearchIterator{iter, d.reads}, nil
}

type countedSearchIterator struct {
	fs.DirectoryIterator
	reads map[string]int
}

func (i countedSearchIterator) Next(ctx context.Context) (fs.Entry, error) {
	entry, err := i.DirectoryIterator.Next(ctx)
	if dir, ok := entry.(fs.Directory); ok {
		return countedSearchDir{dir, i.reads}, err
	}

	return entry, err
}

func TestNameSearcherReusesContentButKeepsEveryPath(t *testing.T) {
	t.Parallel()

	root := mockfs.NewDirectory()
	root.AddDir("one", 0o755).AddFile("needle.txt", nil, 0o600)
	root.AddDir("two", 0o755).AddFile("needle.txt", nil, 0o600)

	reads := map[string]int{}
	searcher, err := snapshotfs.NewNameSearcher("needle", false)
	require.NoError(t, err)

	for range 2 {
		var paths []string
		require.NoError(t, searcher.Search(context.Background(), countedSearchDir{root, reads}, func(match snapshotfs.NameMatch) error {
			paths = append(paths, match.Path)
			return nil
		}))
		require.Equal(t, []string{"one/needle.txt", "two/needle.txt"}, paths)
	}

	require.Equal(t, 1, reads[root.Name()])
	require.Equal(t, 1, reads["one"]+reads["two"])
}

func TestNameSearcherCancellationAndErrors(t *testing.T) {
	t.Parallel()

	root := mockfs.NewDirectory()
	root.AddFile("needle1", nil, 0o600)
	root.AddFile("needle2", nil, 0o600)

	searcher, err := snapshotfs.NewNameSearcher("needle", false)
	require.NoError(t, err)

	stop := errors.New("stop visiting")
	cachedRoot := countedSearchDir{root, map[string]int{}}
	require.ErrorIs(t, searcher.Search(context.Background(), cachedRoot, func(snapshotfs.NameMatch) error { return stop }), stop)

	var matches int
	require.NoError(t, searcher.Search(context.Background(), cachedRoot, func(snapshotfs.NameMatch) error { matches++; return nil }))
	require.Equal(t, 2, matches, "a stopped search must not cache partial results")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, searcher.Search(ctx, root, func(snapshotfs.NameMatch) error { t.Fatal("canceled search called visitor"); return nil }), context.Canceled)

	readErr := errors.New("cannot read archived directory")
	root.FailReaddir(readErr)
	require.ErrorIs(t, searcher.Search(context.Background(), root, func(snapshotfs.NameMatch) error { return nil }), readErr)

	file := mockfs.NewFile("needle", nil, 0o600)
	require.NoError(t, searcher.Search(context.Background(), file, func(match snapshotfs.NameMatch) error {
		require.Equal(t, ".", match.Path)
		return nil
	}))
}

func TestNameSearcherStreamsLargeResults(t *testing.T) {
	t.Parallel()

	root := mockfs.NewDirectory()
	for i := range 5000 {
		root.AddFile(fmt.Sprintf("needle-%05d", i), nil, 0o600)
	}

	reads := map[string]int{}
	searcher, err := snapshotfs.NewNameSearcher("needle", false)
	require.NoError(t, err)

	for range 2 {
		count := 0
		require.NoError(t, searcher.Search(context.Background(), countedSearchDir{root, reads}, func(snapshotfs.NameMatch) error {
			count++

			return nil
		}))
		require.Equal(t, 5000, count, "cache limits must not truncate search results")
	}

	require.Equal(t, 2, reads[root.Name()], "a result set larger than the cache must be streamed again")
}

func TestNameSearcherCancellationDuringCachedReplay(t *testing.T) {
	t.Parallel()

	root := mockfs.NewDirectory()
	root.AddFile("needle1", nil, 0o600)
	root.AddFile("needle2", nil, 0o600)

	reads := map[string]int{}
	searcher, err := snapshotfs.NewNameSearcher("needle", false)
	require.NoError(t, err)
	require.NoError(t, searcher.Search(context.Background(), countedSearchDir{root, reads}, func(snapshotfs.NameMatch) error { return nil }))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 0
	require.ErrorIs(t, searcher.Search(ctx, countedSearchDir{root, reads}, func(snapshotfs.NameMatch) error {
		count++

		cancel()

		return nil
	}), context.Canceled)
	require.Equal(t, 1, count)
	require.Equal(t, 1, reads[root.Name()], "the second search must use the cached directory")
}
