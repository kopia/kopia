package block

import (
	"bytes"
	"context"
	"fmt"
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

// CompactIndexes performs compaction of index blocks ensuring that # of small blocks is between minSmallBlockCount and maxSmallBlockCount
func (bm *Manager) CompactIndexes(ctx context.Context, opt CompactOptions) error {
	log.Debugf("CompactIndexes(%+v)", opt)
	if opt.MaxSmallBlocks < opt.MinSmallBlocks {
		return fmt.Errorf("invalid block counts")
	}

	indexBlocks, _, err := bm.loadPackIndexesUnlocked(ctx)
	if err != nil {
		return errors.Wrap(err, "error loading indexes")
	}

	blocksToCompact := bm.getBlocksToCompact(indexBlocks, opt)

	if err := bm.compactAndDeleteIndexBlocks(ctx, blocksToCompact, opt); err != nil {
		log.Warningf("error performing quick compaction: %v", err)
	}

	return nil
}

func (bm *Manager) getBlocksToCompact(indexBlocks []IndexInfo, opt CompactOptions) []IndexInfo {
	var nonCompactedBlocks []IndexInfo
	var totalSizeNonCompactedBlocks int64

	var verySmallBlocks []IndexInfo
	var totalSizeVerySmallBlocks int64

	var mediumSizedBlocks []IndexInfo
	var totalSizeMediumSizedBlocks int64

	for _, b := range indexBlocks {
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

func (bm *Manager) compactAndDeleteIndexBlocks(ctx context.Context, indexBlocks []IndexInfo, opt CompactOptions) error {
	if len(indexBlocks) <= 1 {
		return nil
	}
	formatLog.Debugf("compacting %v blocks", len(indexBlocks))
	t0 := time.Now()

	bld := make(packIndexBuilder)
	for _, indexBlock := range indexBlocks {
		if err := bm.addIndexBlocksToBuilder(ctx, bld, indexBlock, opt); err != nil {
			return err
		}
	}

	var buf bytes.Buffer
	if err := bld.Build(&buf); err != nil {
		return errors.Wrap(err, "unable to build an index")
	}

	compactedIndexBlock, err := bm.writePackIndexesNew(ctx, buf.Bytes())
	if err != nil {
		return errors.Wrap(err, "unable to write compacted indexes")
	}

	formatLog.Debugf("wrote compacted index (%v bytes) in %v", compactedIndexBlock, time.Since(t0))

	for _, indexBlock := range indexBlocks {
		if indexBlock.FileName == compactedIndexBlock {
			continue
		}

		bm.listCache.deleteListCache(ctx)
		if err := bm.st.DeleteBlock(ctx, indexBlock.FileName); err != nil {
			log.Warningf("unable to delete compacted block %q: %v", indexBlock.FileName, err)
		}
	}

	return nil
}

func (bm *Manager) addIndexBlocksToBuilder(ctx context.Context, bld packIndexBuilder, indexBlock IndexInfo, opt CompactOptions) error {
	data, err := bm.getPhysicalBlockInternal(ctx, indexBlock.FileName)
	if err != nil {
		return err
	}

	index, err := openPackIndex(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("unable to open index block %q: %v", indexBlock, err)
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
