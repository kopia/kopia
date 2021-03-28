package blobtesting

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// VerifyStorage verifies the behavior of the specified storage.
// nolint:gocyclo,thelper
func VerifyStorage(ctx context.Context, t *testing.T, r blob.Storage) {
	blocks := []struct {
		blk      blob.ID
		contents []byte
	}{
		{blk: "abcdbbf4f0507d054ed5a80a5b65086f602b", contents: []byte{}},
		{blk: "zxce0e35630770c54668a8cfb4e414c6bf8f", contents: []byte{1}},
		{blk: "abff4585856ebf0748fd989e1dd623a8963d", contents: bytes.Repeat([]byte{1}, 1000)},
		{blk: "abgc3dca496d510f492c858a2df1eb824e62", contents: bytes.Repeat([]byte{1}, 10000)},
		{blk: "kopia.repository", contents: bytes.Repeat([]byte{2}, 100)},
	}

	// First verify that blocks don't exist.
	t.Run("VerifyBlobsNotFound", func(t *testing.T) {
		for _, b := range blocks {
			b := b

			t.Run(string(b.blk), func(t *testing.T) {
				t.Parallel()

				AssertGetBlobNotFound(ctx, t, r, b.blk)
				AssertGetMetadataNotFound(ctx, t, r, b.blk)
			})
		}
	})

	if err := r.DeleteBlob(ctx, "no-such-blob"); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
		t.Errorf("invalid error when deleting non-existent blob: %v", err)
	}

	initialAddConcurrency := 2
	if os.Getenv("CI") != "" {
		initialAddConcurrency = 4
	}

	// Now add blocks.
	t.Run("AddBlobs", func(t *testing.T) {
		for _, b := range blocks {
			for i := 0; i < initialAddConcurrency; i++ {
				b := b

				t.Run(fmt.Sprintf("%v-%v", b.blk, i), func(t *testing.T) {
					t.Parallel()

					if err := r.PutBlob(ctx, b.blk, gather.FromSlice(b.contents)); err != nil {
						t.Fatalf("can't put blob: %v", err)
					}
				})
			}
		}
	})

	t.Run("GetBlobs", func(t *testing.T) {
		for _, b := range blocks {
			b := b

			t.Run(string(b.blk), func(t *testing.T) {
				t.Parallel()

				AssertGetBlob(ctx, t, r, b.blk, b.contents)
			})
		}
	})

	AssertListResults(ctx, t, r, "", blocks[0].blk, blocks[1].blk, blocks[2].blk, blocks[3].blk, blocks[4].blk)
	AssertListResults(ctx, t, r, "ab", blocks[0].blk, blocks[2].blk, blocks[3].blk)

	t.Run("OverwriteBlobs", func(t *testing.T) {
		for _, b := range blocks {
			b := b

			t.Run(string(b.blk), func(t *testing.T) {
				t.Parallel()

				if err := r.PutBlob(ctx, b.blk, gather.FromSlice(b.contents)); err != nil {
					t.Errorf("can't put blob: %v", err)
				}

				AssertGetBlob(ctx, t, r, b.blk, b.contents)
			})
		}
	})

	ts := time.Date(2020, 1, 1, 15, 30, 45, 0, time.UTC)

	t.Run("SetTime", func(t *testing.T) {
		for _, b := range blocks {
			b := b

			t.Run(string(b.blk), func(t *testing.T) {
				t.Parallel()

				if err := r.SetTime(ctx, b.blk, ts); errors.Is(err, blob.ErrSetTimeUnsupported) {
					return
				}

				md, err := r.GetMetadata(ctx, b.blk)
				if err != nil {
					t.Errorf("unable to get blob metadata")
				}

				if got, want := md.Timestamp, ts; !got.Equal(want) {
					t.Errorf("invalid time after SetTme(): %vm want %v", got, want)
				}
			})
		}
	})

	if err := r.DeleteBlob(ctx, blocks[0].blk); err != nil {
		t.Errorf("unable to delete block: %v", err)
	}

	if err := r.DeleteBlob(ctx, blocks[0].blk); err != nil {
		t.Errorf("invalid error when deleting deleted block: %v", err)
	}

	AssertListResults(ctx, t, r, "ab", blocks[2].blk, blocks[3].blk)
	AssertListResults(ctx, t, r, "", blocks[1].blk, blocks[2].blk, blocks[3].blk, blocks[4].blk)
}

// AssertConnectionInfoRoundTrips verifies that the ConnectionInfo returned by a given storage can be used to create
// equivalent storage.
// nolint:thelper
func AssertConnectionInfoRoundTrips(ctx context.Context, t *testing.T, s blob.Storage) {
	ci := s.ConnectionInfo()

	s2, err := blob.NewStorage(ctx, ci)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	ci2 := s2.ConnectionInfo()
	if !reflect.DeepEqual(ci, ci2) {
		t.Errorf("connection info does not round-trip: %v vs %v", ci, ci2)
	}

	if err := s2.Close(ctx); err != nil {
		t.Errorf("unable to close storage: %v", err)
	}
}
