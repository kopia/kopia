package content

import (
	"bytes"
	"context"
	"time"

	"github.com/pkg/errors"
)

var autoCompactionOptions = CompactOptions{
	MinSmallBlobs: 4 * parallelFetches,
	MaxSmallBlobs: 64,
}

// CompactOptions provides options for compaction
type CompactOptions struct {
	MinSmallBlobs        int
	MaxSmallBlobs        int
	AllIndexes           bool
	SkipDeletedOlderThan time.Duration
}

// CompactIndexes performs compaction of index blobs ensuring that # of small contents is between minSmallContentCount and maxSmallContentCount
func (bm *Manager) CompactIndexes(ctx context.Context, opt CompactOptions) error {
	log.Debugf("CompactIndexes(%+v)", opt)
	if opt.MaxSmallBlobs < opt.MinSmallBlobs {
		return errors.Errorf("invalid content counts")
	}

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
	var nonCompactedContents []IndexBlobInfo
	var totalSizeNonCompactedContents int64

	var verySmallContents []IndexBlobInfo
	var totalSizeVerySmallContents int64

	var mediumSizedContents []IndexBlobInfo
	var totalSizeMediumSizedContents int64

	for _, b := range indexBlobs {
		if b.Length > int64(bm.maxPackSize) && !opt.AllIndexes {
			continue
		}

		nonCompactedContents = append(nonCompactedContents, b)
		if b.Length < int64(bm.maxPackSize/20) {
			verySmallContents = append(verySmallContents, b)
			totalSizeVerySmallContents += b.Length
		} else {
			mediumSizedContents = append(mediumSizedContents, b)
			totalSizeMediumSizedContents += b.Length
		}
		totalSizeNonCompactedContents += b.Length
	}

	if len(nonCompactedContents) < opt.MinSmallBlobs {
		// current count is below min allowed - nothing to do
		formatLog.Debugf("no small contents to compact")
		return nil
	}

	if len(verySmallContents) > len(nonCompactedContents)/2 && len(mediumSizedContents)+1 < opt.MinSmallBlobs {
		formatLog.Debugf("compacting %v very small contents", len(verySmallContents))
		return verySmallContents
	}

	formatLog.Debugf("compacting all %v non-compacted contents", len(nonCompactedContents))
	return nonCompactedContents
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
