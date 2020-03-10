package content

import (
	"bytes"
	"context"
	"time"

	"github.com/pkg/errors"
)

const verySmallContentFraction = 20 // blobs less than 1/verySmallContentFraction of maxPackSize are considered 'very small'

var autoCompactionOptions = CompactOptions{
	MaxSmallBlobs: 4 * parallelFetches, // nolint:gomnd
}

// CompactOptions provides options for compaction
type CompactOptions struct {
	MaxSmallBlobs        int
	AllIndexes           bool
	SkipDeletedOlderThan time.Duration
}

// CompactIndexes performs compaction of index blobs ensuring that # of small index blobs is below opt.maxSmallBlobs
func (bm *Manager) CompactIndexes(ctx context.Context, opt CompactOptions) error {
	log(ctx).Debugf("CompactIndexes(%+v)", opt)

	bm.lock()
	defer bm.unlock()

	indexBlobs, _, err := bm.loadPackIndexesUnlocked(ctx)
	if err != nil {
		return errors.Wrap(err, "error loading indexes")
	}

	contentsToCompact := bm.getContentsToCompact(ctx, indexBlobs, opt)

	if err := bm.compactAndDeleteIndexBlobs(ctx, contentsToCompact, opt); err != nil {
		log(ctx).Warningf("error performing quick compaction: %v", err)
	}

	return nil
}

func (bm *Manager) getContentsToCompact(ctx context.Context, indexBlobs []IndexBlobInfo, opt CompactOptions) []IndexBlobInfo {
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

func (bm *Manager) compactAndDeleteIndexBlobs(ctx context.Context, indexBlobs []IndexBlobInfo, opt CompactOptions) error {
	if len(indexBlobs) <= 1 {
		return nil
	}

	formatLog(ctx).Debugf("compacting %v contents", len(indexBlobs))

	t0 := time.Now() // allow:no-inject-time
	bld := make(packIndexBuilder)

	for _, indexBlob := range indexBlobs {
		if err := bm.addIndexBlobsToBuilder(ctx, bld, indexBlob, opt); err != nil {
			return err
		}
	}

	var buf bytes.Buffer
	if err := bld.Build(&buf); err != nil {
		return errors.Wrap(err, "unable to build an index")
	}

	compactedIndexBlob, err := bm.writePackIndexesNew(ctx, buf.Bytes())
	if err != nil {
		return errors.Wrap(err, "unable to write compacted indexes")
	}

	formatLog(ctx).Debugf("wrote compacted index (%v bytes) in %v", compactedIndexBlob, time.Since(t0)) // allow:no-inject-time

	for _, indexBlob := range indexBlobs {
		if indexBlob.BlobID == compactedIndexBlob {
			continue
		}

		bm.listCache.deleteListCache()

		if err := bm.st.DeleteBlob(ctx, indexBlob.BlobID); err != nil {
			log(ctx).Warningf("unable to delete compacted blob %q: %v", indexBlob.BlobID, err)
		}
	}

	return nil
}

func (bm *Manager) addIndexBlobsToBuilder(ctx context.Context, bld packIndexBuilder, indexBlob IndexBlobInfo, opt CompactOptions) error {
	data, err := bm.getIndexBlobInternal(ctx, indexBlob.BlobID)
	if err != nil {
		return err
	}

	index, err := openPackIndex(bytes.NewReader(data))
	if err != nil {
		return errors.Wrapf(err, "unable to open index blob %q", indexBlob)
	}

	_ = index.Iterate("", func(i Info) error {
		if i.Deleted && opt.SkipDeletedOlderThan > 0 && bm.timeNow().Sub(i.Timestamp()) > opt.SkipDeletedOlderThan {
			log(ctx).Debugf("skipping content %v deleted at %v", i.ID, i.Timestamp())
			return nil
		}
		bld.Add(i)
		return nil
	})

	return nil
}
