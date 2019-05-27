package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/kopia/repo/internal/storagetesting"
	"github.com/kopia/repo/storage"
)

func TestListAllBlocksConsistent(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	st := storagetesting.NewMapStorage(data, nil, time.Now)
	st.PutBlock(ctx, "foo1", []byte{1, 2, 3}) //nolint:errcheck
	st.PutBlock(ctx, "foo2", []byte{1, 2, 3}) //nolint:errcheck
	st.PutBlock(ctx, "foo3", []byte{1, 2, 3}) //nolint:errcheck

	// set up faulty storage that will add a block while a scan is in progress.
	f := &storagetesting.FaultyStorage{
		Base: st,
		Faults: map[string][]*storagetesting.Fault{
			"ListBlocksItem": {
				{ErrCallback: func() error {
					st.PutBlock(ctx, "foo0", []byte{1, 2, 3}) //nolint:errcheck
					return nil
				}},
			},
		},
	}

	r, err := storage.ListAllBlocksConsistent(ctx, f, "foo", 3)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	// make sure we get the list with 4 items, not 3.
	if got, want := len(r), 4; got != want {
		t.Errorf("unexpected list result count: %v, want %v", got, want)
	}
}

func TestListAllBlocksConsistentEmpty(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	st := storagetesting.NewMapStorage(data, nil, time.Now)

	r, err := storage.ListAllBlocksConsistent(ctx, st, "foo", 3)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if got, want := len(r), 0; got != want {
		t.Errorf("unexpected list result count: %v, want %v", got, want)
	}
}
