package blobtesting

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/blob"
)

const maxTimeDiffBetweenGetAndList = 5 * time.Second

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

	half := int64(len(expected) / 2)
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

// AssertInvalidOffsetLength verifies that the given combination of (offset,length) fails on GetBlob().
func AssertInvalidOffsetLength(ctx context.Context, t *testing.T, s blob.Storage, blobID blob.ID, offset, length int64) {
	t.Helper()

	if _, err := s.GetBlob(ctx, blobID, offset, length); err == nil {
		t.Errorf("GetBlob(%v,%v,%v) did not return error for invalid offset/length", blobID, offset, length)
	}
}

// AssertGetBlobNotFound asserts that GetBlob() for specified blobID returns ErrNotFound.
func AssertGetBlobNotFound(ctx context.Context, t *testing.T, s blob.Storage, blobID blob.ID) {
	t.Helper()

	b, err := s.GetBlob(ctx, blobID, 0, -1)
	if !errors.Is(err, blob.ErrBlobNotFound) || b != nil {
		t.Errorf("GetBlob(%v) returned %v, %v but expected ErrNotFound", blobID, b, err)
	}
}

// AssertGetMetadataNotFound asserts that GetMetadata() for specified blobID returns ErrNotFound.
func AssertGetMetadataNotFound(ctx context.Context, t *testing.T, s blob.Storage, blobID blob.ID) {
	t.Helper()

	_, err := s.GetMetadata(ctx, blobID)
	if !errors.Is(err, blob.ErrBlobNotFound) {
		t.Errorf("GetMetadata(%v) returned %v but expected ErrNotFound", blobID, err)
	}
}

// AssertListResults asserts that the list results with given prefix return the specified list of names in order.
func AssertListResults(ctx context.Context, t *testing.T, s blob.Storage, prefix blob.ID, want ...blob.ID) {
	t.Helper()

	var names []blob.ID

	if err := s.ListBlobs(ctx, prefix, func(m blob.Metadata) error {
		names = append(names, m.BlobID)

		m2, err := s.GetMetadata(ctx, m.BlobID)
		if err != nil {
			t.Errorf("GetMetadata() failed: %v", err)
		}

		if got, want := m2.BlobID, m.BlobID; got != want {
			t.Errorf("invalid blob ID on %v: %v, want %v", m.BlobID, got, want)
		}

		if got, want := m2.Length, m.Length; got != want {
			t.Errorf("invalid length on %v: %v, want %v", m.BlobID, got, want)
		}

		timeDiff := m2.Timestamp.Sub(m.Timestamp)
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}

		// truncated time comparison, because some providers return different precision of time in list vs get
		if timeDiff > maxTimeDiffBetweenGetAndList {
			t.Errorf("invalid timestamp on %v: getmetadata returned %v, list returned %v", m.BlobID, m2.Timestamp, m.Timestamp)
		}

		return nil
	}); err != nil {
		t.Errorf("err: %v", err)
	}

	names = sorted(names)
	want = sorted(want)

	if !reflect.DeepEqual(names, want) {
		t.Errorf("ListBlobs(%v) returned %v, but wanted %v", prefix, names, want)
	}
}

// AssertListResultsIDs asserts that the list results with given prefix return the specified list of names.
func AssertListResultsIDs(ctx context.Context, t *testing.T, s blob.Storage, prefix blob.ID, want ...blob.ID) {
	t.Helper()

	var names []blob.ID

	if err := s.ListBlobs(ctx, prefix, func(m blob.Metadata) error {
		names = append(names, m.BlobID)
		return nil
	}); err != nil {
		t.Errorf("err: %v", err)
	}

	require.ElementsMatch(t, names, want)
}

func sorted(s []blob.ID) []blob.ID {
	x := append([]blob.ID(nil), s...)
	sort.Slice(x, func(i, j int) bool {
		return x[i] < x[j]
	})

	return x
}
