package block

import (
	"bytes"
	"context"
	"time"

	"github.com/pkg/errors"
)

var autoCompactionOptions = CompactOptions{
	MinSmallBlocks: 4 * parallelFetches,
	MaxSmallBlocks: 64,
}

// CompactOptions provides options for compaction
type CompactOptions struct {
	MinSmallBlocks       int
	MaxSmallBlocks       int
	AllBlocks            bool
	SkipDeletedOlderThan time.Duration
}

// CompactIndexes performs compaction of index blobs ensuring that # of small blocks is between minSmallBlockCount and maxSmallBlockCount
func (bm *Manager) CompactIndexes(ctx context.Context, opt CompactOptions) error {
	log.Debugf("CompactIndexes(%+v)", opt)
	if opt.MaxSmallBlocks < opt.MinSmallBlocks {
		return errors.Errorf("invalid block counts")
	}

	indexBlobs, _, err := bm.loadPackIndexesUnlocked(ctx)
	if err != nil {
		return errors.Wrap(err, "error loading indexes")
	}

	blocksToCompact := bm.getBlocksToCompact(indexBlobs, opt)

	if err := bm.compactAndDeleteIndexBlobs(ctx, blocksToCompact, opt); err != nil {
		log.Warningf("error performing quick compaction: %v", err)
	}

	return nil
}

func (bm *Manager) getBlocksToCompact(indexBlobs []IndexBlobInfo, opt CompactOptions) []IndexBlobInfo {
	var nonCompactedBlocks []IndexBlobInfo
	var totalSizeNonCompactedBlocks int64

	var verySmallBlocks []IndexBlobInfo
	var totalSizeVerySmallBlocks int64

	var mediumSizedBlocks []IndexBlobInfo
	var totalSizeMediumSizedBlocks int64

	for _, b := range indexBlobs {
		if b.Length > int64(bm.maxPackSize) && !opt.AllBlocks {
			continue
		}

		nonCompactedBlocks = append(nonCompactedBlocks, b)
		if b.Length < int64(bm.maxPackSize/20) {
			verySmallBlocks = append(verySmallBlocks, b)
			totalSizeVerySmallBlocks += b.Length
		} else {
			mediumSizedBlocks = append(mediumSizedBlocks, b)
			totalSizeMediumSizedBlocks += b.Length
		}
		totalSizeNonCompactedBlocks += b.Length
	}

	if len(nonCompactedBlocks) < opt.MinSmallBlocks {
		// current count is below min allowed - nothing to do
		formatLog.Debugf("no small blocks to compact")
		return nil
	}

	if len(verySmallBlocks) > len(nonCompactedBlocks)/2 && len(mediumSizedBlocks)+1 < opt.MinSmallBlocks {
		formatLog.Debugf("compacting %v very small blocks", len(verySmallBlocks))
		return verySmallBlocks
	}

	formatLog.Debugf("compacting all %v non-compacted blocks", len(nonCompactedBlocks))
	return nonCompactedBlocks
}

func (bm *Manager) compactAndDeleteIndexBlobs(ctx context.Context, indexBlobs []IndexBlobInfo, opt CompactOptions) error {
	if len(indexBlobs) <= 1 {
		return nil
	}
	formatLog.Debugf("compacting %v blocks", len(indexBlobs))
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

		bm.listCache.deleteListCache(ctx)
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
			log.Debugf("skipping block %v deleted at %v", i.BlockID, i.Timestamp())
			return nil
		}
		bld.Add(i)
		return nil
	})

	return nil
}
