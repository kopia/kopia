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
	log.Debugf("CompactIndexes(%+v)", opt)

	bm.lock()
	defer bm.unlock()

	indexBlobs, _, err := bm.loadPackIndexesUnlocked(ctx)
	if err != nil {
		return errors.Wrap(err, "error loading indexes")
	}

	contentsToCompact := bm.getContentsToCompact(indexBlobs, opt)

	if err := bm.compactAndDeleteIndexBlobs(ctx, contentsToCompact, opt); err != nil {
		log.Warningf("error performing quick compaction: %v", err)
	}

	return nil
}

func (bm *Manager) getContentsToCompact(indexBlobs []IndexBlobInfo, opt CompactOptions) []IndexBlobInfo {
	var nonCompactedBlobs, verySmallBlobs, mediumSizedBlobs []IndexBlobInfo

	var totalSizeNonCompactedBlobs, totalSizeVerySmallBlobs, totalSizeMediumSizedBlobs int64

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
			mediumSizedBlobs = append(mediumSizedBlobs, b)
			totalSizeMediumSizedBlobs += b.Length
		}
	}

	if len(nonCompactedBlobs) < opt.MaxSmallBlobs {
		// current count is below min allowed - nothing to do
		formatLog.Debugf("no small contents to compact")
		return nil
	}

	if len(verySmallBlobs) > len(nonCompactedBlobs)/2 && len(mediumSizedBlobs)+1 < opt.MaxSmallBlobs {
		formatLog.Debugf("compacting %v very small contents", len(verySmallBlobs))
		return verySmallBlobs
	}

	formatLog.Debugf("compacting all %v non-compacted contents", len(nonCompactedBlobs))

	return nonCompactedBlobs
}

func (bm *Manager) compactAndDeleteIndexBlobs(ctx context.Context, indexBlobs []IndexBlobInfo, opt CompactOptions) error {
	if len(indexBlobs) <= 1 {
		return nil
	}

	formatLog.Debugf("compacting %v contents", len(indexBlobs))

	t0 := time.Now()
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

	formatLog.Debugf("wrote compacted index (%v bytes) in %v", compactedIndexBlob, time.Since(t0))

	for _, indexBlob := range indexBlobs {
		if indexBlob.BlobID == compactedIndexBlob {
			continue
		}

		bm.listCache.deleteListCache()

		if err := bm.st.DeleteBlob(ctx, indexBlob.BlobID); err != nil {
			log.Warningf("unable to delete compacted blob %q: %v", indexBlob.BlobID, err)
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
		if i.Deleted && opt.SkipDeletedOlderThan > 0 && time.Since(i.Timestamp()) > opt.SkipDeletedOlderThan {
			log.Debugf("skipping content %v deleted at %v", i.ID, i.Timestamp())
			return nil
		}
		bld.Add(i)
		return nil
	})

	return nil
}
