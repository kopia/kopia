// Package indexblob manages sets of active index blobs.
package indexblob

import (
	"context"
	"time"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
)

// Manager is the API of index blob manager as used by content manager.
type Manager interface {
	WriteIndexBlobs(ctx context.Context, data []gather.Bytes, suffix blob.ID) ([]blob.Metadata, error)
	ListActiveIndexBlobs(ctx context.Context) ([]Metadata, time.Time, error)
	Compact(ctx context.Context, opts CompactOptions) error
	Invalidate()
}

// CompactOptions provides options for compaction.
type CompactOptions struct {
	MaxSmallBlobs                    int
	AllIndexes                       bool
	DropDeletedBefore                time.Time
	DropContents                     []index.ID
	DisableEventualConsistencySafety bool
}

func (co *CompactOptions) maxEventualConsistencySettleTime() time.Duration {
	if co.DisableEventualConsistencySafety {
		return 0
	}

	return defaultEventualConsistencySettleTime
}

// DefaultIndexShardSize is the maximum number of items in an index shard.
// It is less than 2^24, which lets V1 index use 24-bit/3-byte indexes.
const DefaultIndexShardSize = 16e6

const verySmallContentFraction = 20 // blobs less than 1/verySmallContentFraction of maxPackSize are considered 'very small'

func addBlobsToIndex(ndx map[blob.ID]*Metadata, blobs []blob.Metadata) {
	for _, it := range blobs {
		if ndx[it.BlobID] == nil {
			ndx[it.BlobID] = &Metadata{
				Metadata: blob.Metadata{
					BlobID:    it.BlobID,
					Length:    it.Length,
					Timestamp: it.Timestamp,
				},
			}
		}
	}
}
