package blobtesting

import (
	"bytes"
	"context"
	"reflect"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// VerifyStorage verifies the behavior of the specified storage.
func VerifyStorage(ctx context.Context, t testingT, r blob.Storage) {
	t.Helper()

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
	for _, b := range blocks {
		AssertGetBlobNotFound(ctx, t, r, b.blk)
	}

	// Now add blocks.
	for _, b := range blocks {
		if err := r.PutBlob(ctx, b.blk, gather.FromSlice(b.contents)); err != nil {
			t.Errorf("can't put blob: %v", err)
		}

		AssertGetBlob(ctx, t, r, b.blk, b.contents)
	}

	AssertListResults(ctx, t, r, "", blocks[0].blk, blocks[1].blk, blocks[2].blk, blocks[3].blk, blocks[4].blk)
	AssertListResults(ctx, t, r, "ab", blocks[0].blk, blocks[2].blk, blocks[3].blk)

	// Overwrite blocks.
	for _, b := range blocks {
		if err := r.PutBlob(ctx, b.blk, gather.FromSlice(b.contents)); err != nil {
			t.Errorf("can't put blob: %v", err)
		}

		AssertGetBlob(ctx, t, r, b.blk, b.contents)
	}

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
// equivalent storage
func AssertConnectionInfoRoundTrips(ctx context.Context, t testingT, s blob.Storage) {
	t.Helper()

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
