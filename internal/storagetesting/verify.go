package storagetesting

import (
	"bytes"
	"context"
	"testing"

	"github.com/kopia/kopia/storage"
)

// VerifyStorage verifies the behavior of the specified storage.
func VerifyStorage(ctx context.Context, t *testing.T, r storage.Storage) {
	blocks := []struct {
		blk      string
		contents []byte
	}{
		{blk: string("abcdbbf4f0507d054ed5a80a5b65086f602b"), contents: []byte{}},
		{blk: string("zxce0e35630770c54668a8cfb4e414c6bf8f"), contents: []byte{1}},
		{blk: string("abff4585856ebf0748fd989e1dd623a8963d"), contents: bytes.Repeat([]byte{1}, 1000)},
		{blk: string("abgc3dca496d510f492c858a2df1eb824e62"), contents: bytes.Repeat([]byte{1}, 10000)},
	}

	// First verify that blocks don't exist.
	for _, b := range blocks {
		AssertGetBlockNotFound(ctx, t, r, b.blk)
	}

	// Now add blocks.
	for _, b := range blocks {
		if err := r.PutBlock(ctx, b.blk, b.contents); err != nil {
			t.Errorf("can't put block: %v", err)
		}

		AssertGetBlock(ctx, t, r, b.blk, b.contents)
	}

	AssertListResults(ctx, t, r, "ab", blocks[0].blk, blocks[2].blk, blocks[3].blk)
}
