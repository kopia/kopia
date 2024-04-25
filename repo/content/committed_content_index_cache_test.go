package content

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
)

func TestCommittedContentIndexCache_Disk(t *testing.T) {
	t.Parallel()

	ta := faketime.NewClockTimeWithOffset(0)

	testCache(t, &diskCommittedContentIndexCache{testutil.TempDirectory(t), ta.NowFunc(), func() int { return 3 }, testlogging.Printf(t.Logf, ""), DefaultIndexCacheSweepAge}, ta)
}

func TestCommittedContentIndexCache_Memory(t *testing.T) {
	t.Parallel()

	testCache(t, &memoryCommittedContentIndexCache{
		contents:             map[blob.ID]index.Index{},
		v1PerContentOverhead: func() int { return 3 },
	}, nil)
}

//nolint:thelper
func testCache(t *testing.T, cache committedContentIndexCache, fakeTime *faketime.ClockTimeWithOffset) {
	ctx := testlogging.Context(t)

	has, err := cache.hasIndexBlobID(ctx, "ndx1")
	require.NoError(t, err)

	if has {
		t.Fatal("hasIndexBlobID invalid response, expected false")
	}

	if _, err = cache.openIndex(ctx, "ndx1"); err == nil {
		t.Fatal("openIndex unexpectedly succeeded")
	}

	require.NoError(t, cache.addContentToCache(ctx, "ndx1", mustBuildIndex(t, index.Builder{
		mustParseID(t, "c1"): Info{PackBlobID: "p1234", ContentID: mustParseID(t, "c1")},
		mustParseID(t, "c2"): Info{PackBlobID: "p1234", ContentID: mustParseID(t, "c2")},
	})))

	has, err = cache.hasIndexBlobID(ctx, "ndx1")
	require.NoError(t, err)

	if !has {
		t.Fatal("hasIndexBlobID invalid response, expected true")
	}

	require.NoError(t, cache.addContentToCache(ctx, "ndx2", mustBuildIndex(t, index.Builder{
		mustParseID(t, "c3"): Info{PackBlobID: "p2345", ContentID: mustParseID(t, "c3")},
		mustParseID(t, "c4"): Info{PackBlobID: "p2345", ContentID: mustParseID(t, "c4")},
	})))

	require.NoError(t, cache.addContentToCache(ctx, "ndx2", mustBuildIndex(t, index.Builder{
		mustParseID(t, "c3"): Info{PackBlobID: "p2345", ContentID: mustParseID(t, "c3")},
		mustParseID(t, "c4"): Info{PackBlobID: "p2345", ContentID: mustParseID(t, "c4")},
	})))

	ndx1, err := cache.openIndex(ctx, "ndx1")
	require.NoError(t, err)

	ndx2, err := cache.openIndex(ctx, "ndx2")
	require.NoError(t, err)

	var i Info

	ok, err := ndx1.GetInfo(mustParseID(t, "c1"), &i)
	require.True(t, ok)
	require.NoError(t, err)

	if got, want := i.PackBlobID, blob.ID("p1234"); got != want {
		t.Fatalf("unexpected pack blob ID: %v, want %v", got, want)
	}

	require.NoError(t, ndx1.Close())

	ok, err = ndx2.GetInfo(mustParseID(t, "c3"), &i)
	require.True(t, ok)
	require.NoError(t, err)

	if got, want := i.PackBlobID, blob.ID("p2345"); got != want {
		t.Fatalf("unexpected pack blob ID: %v, want %v", got, want)
	}

	// expire leaving only ndx2, this will actually not do anything for disk cache
	// because it relies on passage of time for safety, we'll re-do it in a sec.
	require.NoError(t, cache.expireUnused(ctx, []blob.ID{"ndx2"}))

	if fakeTime != nil {
		fakeTime.Advance(2 * time.Hour)
	}

	// expire leaving only ndx2
	require.NoError(t, cache.expireUnused(ctx, []blob.ID{"ndx2"}))

	has, err = cache.hasIndexBlobID(ctx, "ndx1")
	require.NoError(t, err)

	if has {
		t.Fatal("hasIndexBlobID invalid response, expected false")
	}

	if _, err = cache.openIndex(ctx, "ndx1"); err == nil {
		t.Fatal("openIndex unexpectedly succeeded")
	}
}

func mustBuildIndex(t *testing.T, b index.Builder) gather.Bytes {
	t.Helper()

	var buf bytes.Buffer
	if err := b.Build(&buf, index.Version2); err != nil {
		t.Fatal(err)
	}

	return gather.FromSlice(buf.Bytes())
}

func mustParseID(t *testing.T, s string) ID {
	t.Helper()

	id, err := index.ParseID(s)
	require.NoError(t, err)

	return id
}
