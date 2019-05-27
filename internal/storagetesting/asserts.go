package storagetesting

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/kopia/kopia/repo/storage"
)

// AssertGetBlock asserts that the specified storage block has correct content.
func AssertGetBlock(ctx context.Context, t *testing.T, s storage.Storage, block string, expected []byte) {
	t.Helper()

	b, err := s.GetBlock(ctx, block, 0, -1)
	if err != nil {
		t.Errorf("GetBlock(%v) returned error %v, expected data: %v", block, err, expected)
		return
	}

	if !bytes.Equal(b, expected) {
		t.Errorf("GetBlock(%v) returned %x, but expected %x", block, b, expected)
	}

	half := int64(len(expected) / 2)
	if half == 0 {
		return
	}

	b, err = s.GetBlock(ctx, block, 0, 0)
	if err != nil {
		t.Errorf("GetBlock(%v) returned error %v, expected data: %v", block, err, expected)
		return
	}

	if len(b) != 0 {
		t.Errorf("GetBlock(%v) returned non-zero length: %v", block, len(b))
		return
	}

	b, err = s.GetBlock(ctx, block, 0, half)
	if err != nil {
		t.Errorf("GetBlock(%v) returned error %v, expected data: %v", block, err, expected)
		return
	}

	if !bytes.Equal(b, expected[0:half]) {
		t.Errorf("GetBlock(%v) returned %x, but expected %x", block, b, expected[0:half])
	}

	b, err = s.GetBlock(ctx, block, half, int64(len(expected))-half)
	if err != nil {
		t.Errorf("GetBlock(%v) returned error %v, expected data: %v", block, err, expected)
		return
	}

	if !bytes.Equal(b, expected[len(expected)-int(half):]) {
		t.Errorf("GetBlock(%v) returned %x, but expected %x", block, b, expected[len(expected)-int(half):])
	}

	AssertInvalidOffsetLength(ctx, t, s, block, -3, 1)
	AssertInvalidOffsetLength(ctx, t, s, block, int64(len(expected)), 3)
	AssertInvalidOffsetLength(ctx, t, s, block, int64(len(expected)-1), 3)
	AssertInvalidOffsetLength(ctx, t, s, block, int64(len(expected)+1), 3)
}

// AssertInvalidOffsetLength verifies that the given combination of (offset,length) fails on GetBlock()
func AssertInvalidOffsetLength(ctx context.Context, t *testing.T, s storage.Storage, block string, offset, length int64) {
	if _, err := s.GetBlock(ctx, block, offset, length); err == nil {
		t.Errorf("GetBlock(%v,%v,%v) did not return error for invalid offset/length", block, offset, length)
	}
}

// AssertGetBlockNotFound asserts that GetBlock() for specified storage block returns ErrBlockNotFound.
func AssertGetBlockNotFound(ctx context.Context, t *testing.T, s storage.Storage, block string) {
	t.Helper()

	b, err := s.GetBlock(ctx, block, 0, -1)
	if err != storage.ErrBlockNotFound || b != nil {
		t.Errorf("GetBlock(%v) returned %v, %v but expected ErrBlockNotFound", block, b, err)
	}
}

// AssertListResults asserts that the list results with given prefix return the specified list of names in order.
func AssertListResults(ctx context.Context, t *testing.T, s storage.Storage, prefix string, want ...string) {
	t.Helper()
	var names []string

	if err := s.ListBlocks(ctx, prefix, func(e storage.BlockMetadata) error {
		names = append(names, e.BlockID)
		return nil
	}); err != nil {
		t.Fatalf("err: %v", err)
	}

	names = sorted(names)
	want = sorted(want)

	if !reflect.DeepEqual(names, want) {
		t.Errorf("ListBlocks(%v) returned %v, but wanted %v", prefix, names, want)
	}
}

func sorted(s []string) []string {
	x := append([]string(nil), s...)
	sort.Strings(x)
	return x
}
