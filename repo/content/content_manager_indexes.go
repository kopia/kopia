package content

import (
	"bytes"
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

const verySmallContentFraction = 20 // blobs less than 1/verySmallContentFraction of maxPackSize are considered 'very small'

// CompactOptions provides options for compaction.
type CompactOptions struct {
	MaxSmallBlobs                    int
	AllIndexes                       bool
	DropDeletedBefore                time.Time
	DropContents                     []ID
	DisableEventualConsistencySafety bool
}

func (co *CompactOptions) maxEventualConsistencySettleTime() time.Duration {
	if co.DisableEventualConsistencySafety {
		return 0
	}

	return defaultEventualConsistencySettleTime
}

// CompactIndexes performs compaction of index blobs ensuring that # of small index blobs is below opt.maxSmallBlobs.
func (bm *WriteManager) CompactIndexes(ctx context.Context, opt CompactOptions) error {
	bm.log.Debugf("CompactIndexes(%+v)", opt)

	if err := bm.indexBlobManager.compact(ctx, opt); err != nil {
		return errors.Wrap(err, "error performing compaction")
	}

	// reload indexes after compaction.
	if err := bm.loadPackIndexesUnlocked(ctx); err != nil {
		return errors.Wrap(err, "error re-loading indexes")
	}

	return nil
}

// ParseIndexBlob loads entries in a given index blob and returns them.
func (sm *SharedManager) ParseIndexBlob(ctx context.Context, blobID blob.ID) ([]Info, error) {
	data, err := sm.indexBlobManager.getIndexBlob(ctx, blobID)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting index %q", blobID)
	}

	index, err := openPackIndex(bytes.NewReader(data), uint32(sm.crypter.Encryptor.Overhead()))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open index blob")
	}

	var results []Info

	err = index.Iterate(AllIDs, func(i Info) error {
		results = append(results, i)
		return nil
	})

	return results, errors.Wrap(err, "error iterating index entries")
}

func addBlobsToIndex(ndx map[blob.ID]*IndexBlobInfo, blobs []blob.Metadata) {
	for _, it := range blobs {
		if ndx[it.BlobID] == nil {
			ndx[it.BlobID] = &IndexBlobInfo{
				Metadata: blob.Metadata{
					BlobID:    it.BlobID,
					Length:    it.Length,
					Timestamp: it.Timestamp,
				},
			}
		}
	}
}
