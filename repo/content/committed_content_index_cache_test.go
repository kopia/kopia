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
	"github.com/kopia/kopia/repo/logging"
)

func TestCommittedContentIndexCache_Disk(t *testing.T) {
	t.Parallel()

	ta := faketime.NewClockTimeWithOffset(0)

	testCache(t, &diskCommittedContentIndexCache{testutil.TempDirectory(t), ta.NowFunc(), 3, logging.Printf(t.Logf, "test")}, ta)
}

func TestCommittedContentIndexCache_Memory(t *testing.T) {
	t.Parallel()

	testCache(t, &memoryCommittedContentIndexCache{
		contents:             map[blob.ID]packIndex{},
		v1PerContentOverhead: 3,
	}, nil)
}

// nolint:thelper
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

	require.NoError(t, cache.addContentToCache(ctx, "ndx1", mustBuildPackIndex(t, packIndexBuilder{
		"c1": &InfoStruct{PackBlobID: "p1234", ContentID: "c1"},
		"c2": &InfoStruct{PackBlobID: "p1234", ContentID: "c2"},
	})))

	has, err = cache.hasIndexBlobID(ctx, "ndx1")
	require.NoError(t, err)

	if !has {
		t.Fatal("hasIndexBlobID invalid response, expected true")
	}

	require.NoError(t, cache.addContentToCache(ctx, "ndx2", mustBuildPackIndex(t, packIndexBuilder{
		"c3": &InfoStruct{PackBlobID: "p2345", ContentID: "c3"},
		"c4": &InfoStruct{PackBlobID: "p2345", ContentID: "c4"},
	})))

	require.NoError(t, cache.addContentToCache(ctx, "ndx2", mustBuildPackIndex(t, packIndexBuilder{
		"c3": &InfoStruct{PackBlobID: "p2345", ContentID: "c3"},
		"c4": &InfoStruct{PackBlobID: "p2345", ContentID: "c4"},
	})))

	ndx1, err := cache.openIndex(ctx, "ndx1")
	require.NoError(t, err)

	ndx2, err := cache.openIndex(ctx, "ndx2")
	require.NoError(t, err)

	i, err := ndx1.GetInfo("c1")
	require.NoError(t, err)

	if got, want := i.GetPackBlobID(), blob.ID("p1234"); got != want {
		t.Fatalf("unexpected pack blob ID: %v, want %v", got, want)
	}

	require.NoError(t, ndx1.Close())

	i, err = ndx2.GetInfo("c3")
	require.NoError(t, err)

	if got, want := i.GetPackBlobID(), blob.ID("p2345"); got != want {
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

func mustBuildPackIndex(t *testing.T, b packIndexBuilder) gather.Bytes {
	t.Helper()

	var buf bytes.Buffer
	if err := b.Build(&buf, v2IndexVersion); err != nil {
		t.Fatal(err)
	}

	return gather.FromSlice(buf.Bytes())
}
