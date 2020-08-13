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
	MaxSmallBlobs     int
	AllIndexes        bool
	DropDeletedBefore time.Time
	DropContents      []ID
}

// CompactIndexes performs compaction of index blobs ensuring that # of small index blobs is below opt.maxSmallBlobs.
func (bm *Manager) CompactIndexes(ctx context.Context, opt CompactOptions) error {
	log(ctx).Debugf("CompactIndexes(%+v)", opt)

	bm.lock()
	defer bm.unlock()

	indexBlobs, _, err := bm.loadPackIndexesUnlocked(ctx)
	if err != nil {
		return errors.Wrap(err, "error loading indexes")
	}

	blobsToCompact := bm.getBlobsToCompact(ctx, indexBlobs, opt)

	if err := bm.compactIndexBlobs(ctx, blobsToCompact, opt); err != nil {
		return errors.Wrap(err, "error performing compaction")
	}

	return bm.indexBlobManager.cleanup(ctx)
}

func (bm *Manager) getBlobsToCompact(ctx context.Context, indexBlobs []IndexBlobInfo, opt CompactOptions) []IndexBlobInfo {
	var nonCompactedBlobs, verySmallBlobs []IndexBlobInfo

	var totalSizeNonCompactedBlobs, totalSizeVerySmallBlobs, totalSizeMediumSizedBlobs int64

	var mediumSizedBlobCount int

	for _, b := range indexBlobs {
		if b.Length > int64(bm.maxPackSize) && !opt.AllIndexes {
			continue
		}

		nonCompactedBlobs = append(nonCompactedBlobs, b)
		totalSizeNonCompactedBlobs += b.Length

		if b.Length < int64(bm.maxPackSize/verySmallContentFraction) {
			verySmallBlobs = append(verySmallBlobs, b)
			totalSizeVerySmallBlobs += b.Length
		} else {
			mediumSizedBlobCount++
			totalSizeMediumSizedBlobs += b.Length
		}
	}

	if len(nonCompactedBlobs) < opt.MaxSmallBlobs {
		// current count is below min allowed - nothing to do
		formatLog(ctx).Debugf("no small contents to compact")
		return nil
	}

	if len(verySmallBlobs) > len(nonCompactedBlobs)/2 && mediumSizedBlobCount+1 < opt.MaxSmallBlobs {
		formatLog(ctx).Debugf("compacting %v very small contents", len(verySmallBlobs))
		return verySmallBlobs
	}

	formatLog(ctx).Debugf("compacting all %v non-compacted contents", len(nonCompactedBlobs))

	return nonCompactedBlobs
}

func (bm *Manager) compactIndexBlobs(ctx context.Context, indexBlobs []IndexBlobInfo, opt CompactOptions) error {
	if len(indexBlobs) <= 1 && opt.DropDeletedBefore.IsZero() && len(opt.DropContents) == 0 {
		return nil
	}

	formatLog(ctx).Debugf("compacting %v index blobs", len(indexBlobs))

	bld := make(packIndexBuilder)

	var inputs, outputs []blob.Metadata

	for _, indexBlob := range indexBlobs {
		if err := bm.addIndexBlobsToBuilder(ctx, bld, indexBlob, opt); err != nil {
			return errors.Wrap(err, "error adding index to builder")
		}

		inputs = append(inputs, indexBlob.Metadata)
	}

	var buf bytes.Buffer
	if err := bld.Build(&buf); err != nil {
		return errors.Wrap(err, "unable to build an index")
	}

	compactedIndexBlob, err := bm.indexBlobManager.writeIndexBlob(ctx, buf.Bytes())
	if err != nil {
		return errors.Wrap(err, "unable to write compacted indexes")
	}

	// compaction wrote index blob that's the same as one of the sources
	// it must be a no-op.
	for _, indexBlob := range indexBlobs {
		if indexBlob.BlobID == compactedIndexBlob.BlobID {
			formatLog(ctx).Debugf("compaction was a no-op")
			return nil
		}
	}

	outputs = append(outputs, compactedIndexBlob)

	if err := bm.indexBlobManager.registerCompaction(ctx, inputs, outputs); err != nil {
		return errors.Wrap(err, "unable to register compaction")
	}

	return nil
}

func (bm *Manager) addIndexBlobsToBuilder(ctx context.Context, bld packIndexBuilder, indexBlob IndexBlobInfo, opt CompactOptions) error {
	data, err := bm.indexBlobManager.getIndexBlob(ctx, indexBlob.BlobID)
	if err != nil {
		return errors.Wrapf(err, "error getting index %q", indexBlob.BlobID)
	}

	index, err := openPackIndex(bytes.NewReader(data))
	if err != nil {
		return errors.Wrapf(err, "unable to open index blob %q", indexBlob)
	}

	_ = index.Iterate(AllIDs, func(i Info) error {
		for _, dc := range opt.DropContents {
			if dc == i.ID {
				log(ctx).Debugf("dropping content %v", i.ID)
				return nil
			}
		}
		if i.Deleted && !opt.DropDeletedBefore.IsZero() && i.Timestamp().Before(opt.DropDeletedBefore) {
			log(ctx).Debugf("skipping content %v deleted at %v", i.ID, i.Timestamp())
			return nil
		}
		bld.Add(i)
		return nil
	})

	return nil
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
