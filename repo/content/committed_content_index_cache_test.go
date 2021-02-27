package content

import (
	"bytes"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
)

func TestCommittedContentIndexCache_Disk(t *testing.T) {
	t.Parallel()

	ta := faketime.NewClockTimeWithOffset(0)

	testCache(t, &diskCommittedContentIndexCache{testutil.TempDirectory(t), ta.NowFunc()}, ta)
}

func TestCommittedContentIndexCache_Memory(t *testing.T) {
	t.Parallel()

	testCache(t, &memoryCommittedContentIndexCache{
		contents: map[blob.ID]packIndex{},
	}, nil)
}

// nolint:thelper
func testCache(t *testing.T, cache committedContentIndexCache, fakeTime *faketime.ClockTimeWithOffset) {
	ctx := testlogging.Context(t)

	has, err := cache.hasIndexBlobID(ctx, "ndx1")
	must(t, err)

	if has {
		t.Fatal("hasIndexBlobID invalid response, expected false")
	}

	if _, err = cache.openIndex(ctx, "ndx1"); err == nil {
		t.Fatal("openIndex unexpectedly succeeded")
	}

	must(t, cache.addContentToCache(ctx, "ndx1", mustBuildPackIndex(t, packIndexBuilder{
		"c1": &Info{PackBlobID: "p1234", ID: "c1"},
		"c2": &Info{PackBlobID: "p1234", ID: "c2"},
	})))

	has, err = cache.hasIndexBlobID(ctx, "ndx1")
	must(t, err)

	if !has {
		t.Fatal("hasIndexBlobID invalid response, expected true")
	}

	must(t, cache.addContentToCache(ctx, "ndx2", mustBuildPackIndex(t, packIndexBuilder{
		"c3": &Info{PackBlobID: "p2345", ID: "c3"},
		"c4": &Info{PackBlobID: "p2345", ID: "c4"},
	})))

	ndx1, err := cache.openIndex(ctx, "ndx1")
	must(t, err)

	ndx2, err := cache.openIndex(ctx, "ndx2")
	must(t, err)

	i, err := ndx1.GetInfo("c1")
	must(t, err)

	if got, want := i.PackBlobID, blob.ID("p1234"); got != want {
		t.Fatalf("unexpected pack blob ID: %v, want %v", got, want)
	}

	must(t, ndx1.Close())

	i, err = ndx2.GetInfo("c3")
	must(t, err)

	if got, want := i.PackBlobID, blob.ID("p2345"); got != want {
		t.Fatalf("unexpected pack blob ID: %v, want %v", got, want)
	}

	// expire leaving only ndx2, this will actually not do anything for disk cache
	// because it relies on passage of time for safety, we'll re-do it in a sec.
	must(t, cache.expireUnused(ctx, []blob.ID{"ndx2"}))

	if fakeTime != nil {
		fakeTime.Advance(2 * time.Hour)
	}

	// expire leaving only ndx2
	must(t, cache.expireUnused(ctx, []blob.ID{"ndx2"}))

	has, err = cache.hasIndexBlobID(ctx, "ndx1")
	must(t, err)

	if has {
		t.Fatal("hasIndexBlobID invalid response, expected false")
	}

	if _, err = cache.openIndex(ctx, "ndx1"); err == nil {
		t.Fatal("openIndex unexpectedly succeeded")
	}
}

func mustBuildPackIndex(t *testing.T, b packIndexBuilder) []byte {
	t.Helper()

	var buf bytes.Buffer
	if err := b.Build(&buf); err != nil {
		t.Fatal(err)
	}

	return buf.Bytes()
}
