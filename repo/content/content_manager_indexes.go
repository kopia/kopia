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

	if err := bm.indexBlobManager.cleanup(ctx, opt.maxEventualConsistencySettleTime()); err != nil {
		return errors.Wrap(err, "error cleaning up index blobs")
	}

	// reload indexes after cleanup.
	if _, _, err := bm.loadPackIndexesUnlocked(ctx); err != nil {
		return errors.Wrap(err, "error re-loading indexes")
	}

	return nil
}

func (sm *SharedManager) getBlobsToCompact(ctx context.Context, indexBlobs []IndexBlobInfo, opt CompactOptions) []IndexBlobInfo {
	var nonCompactedBlobs, verySmallBlobs []IndexBlobInfo

	var totalSizeNonCompactedBlobs, totalSizeVerySmallBlobs, totalSizeMediumSizedBlobs int64

	var mediumSizedBlobCount int

	for _, b := range indexBlobs {
		if b.Length > int64(sm.maxPackSize) && !opt.AllIndexes {
			continue
		}

		nonCompactedBlobs = append(nonCompactedBlobs, b)
		totalSizeNonCompactedBlobs += b.Length

		if b.Length < int64(sm.maxPackSize/verySmallContentFraction) {
			verySmallBlobs = append(verySmallBlobs, b)
			totalSizeVerySmallBlobs += b.Length
		} else {
			mediumSizedBlobCount++
			totalSizeMediumSizedBlobs += b.Length
		}
	}

	if len(nonCompactedBlobs) < opt.MaxSmallBlobs {
		// current count is below min allowed - nothing to do
		log(ctx).Debugf("no small contents to compact")
		return nil
	}

	if len(verySmallBlobs) > len(nonCompactedBlobs)/2 && mediumSizedBlobCount+1 < opt.MaxSmallBlobs {
		log(ctx).Debugf("compacting %v very small contents", len(verySmallBlobs))
		return verySmallBlobs
	}

	log(ctx).Debugf("compacting all %v non-compacted contents", len(nonCompactedBlobs))

	return nonCompactedBlobs
}

func (sm *SharedManager) compactIndexBlobs(ctx context.Context, indexBlobs []IndexBlobInfo, opt CompactOptions) error {
	if len(indexBlobs) <= 1 && opt.DropDeletedBefore.IsZero() && len(opt.DropContents) == 0 {
		return nil
	}

	bld := make(packIndexBuilder)

	var inputs, outputs []blob.Metadata

	for i, indexBlob := range indexBlobs {
		formatLog(ctx).Debugf("compacting-entries[%v/%v] %v", i, len(indexBlobs), indexBlob)

		if err := sm.addIndexBlobsToBuilder(ctx, bld, indexBlob); err != nil {
			return errors.Wrap(err, "error adding index to builder")
		}

		inputs = append(inputs, indexBlob.Metadata)
	}

	// after we built index map in memory, drop contents from it
	// we must do it after all input blobs have been merged, otherwise we may resurrect contents.
	dropContentsFromBuilder(ctx, bld, opt)

	var buf bytes.Buffer
	if err := bld.Build(&buf); err != nil {
		return errors.Wrap(err, "unable to build an index")
	}

	compactedIndexBlob, err := sm.indexBlobManager.writeIndexBlob(ctx, buf.Bytes(), "")
	if err != nil {
		return errors.Wrap(err, "unable to write compacted indexes")
	}

	// compaction wrote index blob that's the same as one of the sources
	// it must be a no-op.
	for _, indexBlob := range indexBlobs {
		if indexBlob.BlobID == compactedIndexBlob.BlobID {
			formatLog(ctx).Debugf("compaction-noop")
			return nil
		}
	}

	outputs = append(outputs, compactedIndexBlob)

	if err := sm.indexBlobManager.registerCompaction(ctx, inputs, outputs, opt.maxEventualConsistencySettleTime()); err != nil {
		return errors.Wrap(err, "unable to register compaction")
	}

	return nil
}

func dropContentsFromBuilder(ctx context.Context, bld packIndexBuilder, opt CompactOptions) {
	for _, dc := range opt.DropContents {
		if _, ok := bld[dc]; ok {
			formatLog(ctx).Debugf("manual-drop-from-index %v", dc)
			delete(bld, dc)
		}
	}

	if !opt.DropDeletedBefore.IsZero() {
		formatLog(ctx).Debugf("drop-content-deleted-before %v", opt.DropDeletedBefore)

		for _, i := range bld {
			if i.Deleted && i.Timestamp().Before(opt.DropDeletedBefore) {
				formatLog(ctx).Debugf("drop-from-index-old-deleted %v %v", i.ID, i.Timestamp())
				delete(bld, i.ID)
			}
		}

		formatLog(ctx).Debugf("finished drop-content-deleted-before %v", opt.DropDeletedBefore)
	}
}

func (sm *SharedManager) addIndexBlobsToBuilder(ctx context.Context, bld packIndexBuilder, indexBlob IndexBlobInfo) error {
	data, err := sm.indexBlobManager.getIndexBlob(ctx, indexBlob.BlobID)
	if err != nil {
		return errors.Wrapf(err, "error getting index %q", indexBlob.BlobID)
	}

	index, err := openPackIndex(bytes.NewReader(data))
	if err != nil {
		return errors.Wrapf(err, "unable to open index blob %q", indexBlob)
	}

	_ = index.Iterate(AllIDs, func(i Info) error {
		bld.Add(i)
		return nil
	})

	return nil
}

// ParseIndexBlob loads entries in a given index blob and returns them.
func (sm *SharedManager) ParseIndexBlob(ctx context.Context, blobID blob.ID) ([]Info, error) {
	data, err := sm.indexBlobManager.getIndexBlob(ctx, blobID)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting index %q", blobID)
	}

	index, err := openPackIndex(bytes.NewReader(data))
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
