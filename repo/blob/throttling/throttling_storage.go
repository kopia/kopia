// Package throttling implements wrapper around blob.Storage that adds throttling to all calls.
// Throttling is performed for both operations and byte counts (upload and download separately).
package throttling

import (
	"context"
	"time"

	"github.com/kopia/kopia/repo/blob"
)

// assume we will need to download ~20 MB for blobs of unknown length, we will refund the difference
// if we guess wrong or acquire more.
const unknownBlobAcquireLength = 20000000

// Throttler implements throttling policy by blocking before certain operations are
// attempted to ensure we don't exceed the desired rate of operations/bytes uploaded/downloaded.
type Throttler interface {
	BeforeOperation(ctx context.Context, op string)

	// BeforeDownload acquires the specified number of downloaded bytes
	// possibly blocking until enough are available.
	BeforeDownload(ctx context.Context, numBytes int64)

	// BeforeUpload acquires the specified number of upload bytes
	// possibly blocking until enough are available.
	BeforeUpload(ctx context.Context, numBytes int64)

	// ReturnUnusedDownloadBytes returns the specified number of unused download bytes.
	ReturnUnusedDownloadBytes(ctx context.Context, numBytes int64)
}

// throttlingStorage.
type throttlingStorage struct {
	blob.Storage
	throttler Throttler
}

func (s *throttlingStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	acquired := length
	if acquired < 0 {
		acquired = unknownBlobAcquireLength
	}

	s.throttler.BeforeOperation(ctx, "GetBlob")
	s.throttler.BeforeDownload(ctx, acquired)

	output.Reset()

	err := s.Storage.GetBlob(ctx, id, offset, length, output)
	downloaded := int64(output.Length())

	if acquired != downloaded {
		if downloaded > acquired {
			// we downloaded more than initially acquired, acquire more which may pause for a bit.
			s.throttler.BeforeDownload(ctx, downloaded-acquired)
		} else {
			// we downloaded less than initially acquired, release extra
			s.throttler.ReturnUnusedDownloadBytes(ctx, acquired-downloaded)
		}
	}

	return err // nolint:wrapcheck
}

func (s *throttlingStorage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	s.throttler.BeforeOperation(ctx, "GetMetadata")

	return s.Storage.GetMetadata(ctx, id) // nolint:wrapcheck
}

func (s *throttlingStorage) ListBlobs(ctx context.Context, blobIDPrefix blob.ID, cb func(bm blob.Metadata) error) error {
	s.throttler.BeforeOperation(ctx, "ListBlobs")
	return s.Storage.ListBlobs(ctx, blobIDPrefix, cb) // nolint:wrapcheck
}

func (s *throttlingStorage) SetTime(ctx context.Context, id blob.ID, t time.Time) error {
	s.throttler.BeforeOperation(ctx, "SetTime")
	return s.Storage.SetTime(ctx, id, t) // nolint:wrapcheck
}

func (s *throttlingStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	s.throttler.BeforeOperation(ctx, "PutBlob")
	s.throttler.BeforeUpload(ctx, int64(data.Length()))

	return s.Storage.PutBlob(ctx, id, data, opts) // nolint:wrapcheck
}

func (s *throttlingStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	s.throttler.BeforeOperation(ctx, "DeleteBlob")
	return s.Storage.DeleteBlob(ctx, id) // nolint:wrapcheck
}

// NewWrapper returns a Storage wrapper that adds retry loop around all operations of the underlying storage.
func NewWrapper(wrapped blob.Storage, throttler Throttler) blob.Storage {
	return &throttlingStorage{wrapped, throttler}
}
