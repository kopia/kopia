package blob_test

import (
	"testing"
	"time"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

func TestListAllBlobsConsistent(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, time.Now)
	st.PutBlob(ctx, "foo1", []byte{1, 2, 3}) //nolint:errcheck
	st.PutBlob(ctx, "foo2", []byte{1, 2, 3}) //nolint:errcheck
	st.PutBlob(ctx, "foo3", []byte{1, 2, 3}) //nolint:errcheck

	// set up faulty storage that will add a blob while a scan is in progress.
	f := &blobtesting.FaultyStorage{
		Base: st,
		Faults: map[string][]*blobtesting.Fault{
			"ListBlobsItem": {
				{ErrCallback: func() error {
					st.PutBlob(ctx, "foo0", []byte{1, 2, 3}) //nolint:errcheck
					return nil
				}},
			},
		},
	}

	r, err := blob.ListAllBlobsConsistent(ctx, f, "foo", 3)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	// make sure we get the list with 4 items, not 3.
	if got, want := len(r), 4; got != want {
		t.Errorf("unexpected list result count: %v, want %v", got, want)
	}
}

func TestListAllBlobsConsistentEmpty(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, time.Now)

	r, err := blob.ListAllBlobsConsistent(ctx, st, "foo", 3)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if got, want := len(r), 0; got != want {
		t.Errorf("unexpected list result count: %v, want %v", got, want)
	}
}
