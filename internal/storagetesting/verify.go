package storagetesting

import (
	"bytes"
	"testing"

	"github.com/kopia/kopia/storage"
)

// VerifyStorage verifies the behavior of the specified storage.
func VerifyStorage(t *testing.T, r storage.Storage) {
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
		if _, err := r.BlockSize(b.blk); err != storage.ErrBlockNotFound {
			t.Errorf("block exists or error: %v %v", b.blk, err)
		}

		AssertBlockExists(t, r, b.blk, false)
		AssertGetBlockNotFound(t, r, b.blk)
	}

	// Now add blocks.
	for _, b := range blocks {
		r.PutBlock(b.blk, b.contents)

		AssertBlockExists(t, r, b.blk, true)
		AssertGetBlock(t, r, b.blk, b.contents)
	}

	// List
	ch, cancel := r.ListBlocks(string("ab"))
	defer cancel()
	e1, ok := <-ch
	if !ok || e1.BlockID != blocks[0].blk {
		t.Errorf("missing result 0")
	}
	e2, ok := <-ch
	if !ok || e2.BlockID != blocks[2].blk {
		t.Errorf("missing result 2")
	}
	e3, ok := <-ch
	if !ok || e3.BlockID != blocks[3].blk {
		t.Errorf("missing result 3")
	}
	e4, ok := <-ch
	if ok {
		t.Errorf("unexpected item: %v", e4)
	}

	if e1.TimeStamp.After(e2.TimeStamp) || e2.TimeStamp.After(e3.TimeStamp) {
		t.Errorf("timings are not sorted: %v %v %v", e1.TimeStamp, e2.TimeStamp, e3.TimeStamp)
	}
}
