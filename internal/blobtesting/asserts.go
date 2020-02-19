package blobtesting

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/kopia/kopia/repo/blob"
)

// AssertGetBlob asserts that the specified BLOB has correct content.
func AssertGetBlob(ctx context.Context, t *testing.T, s blob.Storage, blobID blob.ID, expected []byte) {
	t.Helper()

	b, err := s.GetBlob(ctx, blobID, 0, -1)
	if err != nil {
		t.Errorf("GetBlob(%v) returned error %v, expected data: %v", blobID, err, expected)
		return
	}

	if !bytes.Equal(b, expected) {
		t.Errorf("GetBlob(%v) returned %x, but expected %x", blobID, b, expected)
	}

	half := int64(len(expected) / 2) //nolint:gomnd
	if half == 0 {
		return
	}

	b, err = s.GetBlob(ctx, blobID, 0, 0)
	if err != nil {
		t.Errorf("GetBlob(%v) returned error %v, expected data: %v", blobID, err, expected)
		return
	}

	if len(b) != 0 {
		t.Errorf("GetBlob(%v) returned non-zero length: %v", blobID, len(b))
		return
	}

	b, err = s.GetBlob(ctx, blobID, 0, half)
	if err != nil {
		t.Errorf("GetBlob(%v) returned error %v, expected data: %v", blobID, err, expected)
		return
	}

	if !bytes.Equal(b, expected[0:half]) {
		t.Errorf("GetBlob(%v) returned %x, but expected %x", blobID, b, expected[0:half])
	}

	b, err = s.GetBlob(ctx, blobID, half, int64(len(expected))-half)
	if err != nil {
		t.Errorf("GetBlob(%v) returned error %v, expected data: %v", blobID, err, expected)
		return
	}

	if !bytes.Equal(b, expected[len(expected)-int(half):]) {
		t.Errorf("GetBlob(%v) returned %x, but expected %x", blobID, b, expected[len(expected)-int(half):])
	}

	AssertInvalidOffsetLength(ctx, t, s, blobID, -3, 1)
	AssertInvalidOffsetLength(ctx, t, s, blobID, int64(len(expected)), 3)
	AssertInvalidOffsetLength(ctx, t, s, blobID, int64(len(expected)-1), 3)
	AssertInvalidOffsetLength(ctx, t, s, blobID, int64(len(expected)+1), 3)
}

// AssertInvalidOffsetLength verifies that the given combination of (offset,length) fails on GetBlob()
func AssertInvalidOffsetLength(ctx context.Context, t *testing.T, s blob.Storage, blobID blob.ID, offset, length int64) {
	if _, err := s.GetBlob(ctx, blobID, offset, length); err == nil {
		t.Errorf("GetBlob(%v,%v,%v) did not return error for invalid offset/length", blobID, offset, length)
	}
}

// AssertGetBlobNotFound asserts that GetBlob() for specified blobID returns ErrNotFound.
func AssertGetBlobNotFound(ctx context.Context, t *testing.T, s blob.Storage, blobID blob.ID) {
	t.Helper()

	b, err := s.GetBlob(ctx, blobID, 0, -1)
	if err != blob.ErrBlobNotFound || b != nil {
		t.Errorf("GetBlob(%v) returned %v, %v but expected ErrNotFound", blobID, b, err)
	}
}

// AssertListResults asserts that the list results with given prefix return the specified list of names in order.
func AssertListResults(ctx context.Context, t *testing.T, s blob.Storage, prefix blob.ID, want ...blob.ID) {
	t.Helper()

	var names []blob.ID

	if err := s.ListBlobs(ctx, prefix, func(e blob.Metadata) error {
		names = append(names, e.BlobID)
		return nil
	}); err != nil {
		t.Fatalf("err: %v", err)
	}

	names = sorted(names)
	want = sorted(want)

	if !reflect.DeepEqual(names, want) {
		t.Errorf("ListBlobs(%v) returned %v, but wanted %v", prefix, names, want)
	}
}

func sorted(s []blob.ID) []blob.ID {
	x := append([]blob.ID(nil), s...)
	sort.Slice(x, func(i, j int) bool {
		return x[i] < x[j]
	})

	return x
}
