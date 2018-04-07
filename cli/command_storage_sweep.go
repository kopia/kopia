package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/kopia/kopia/block"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/storage"
)

var (
	storageSweepCommand      = storageCommands.Command("sweep", "Remove unused storage blocks")
	storageSweepAgeThreshold = storageSweepCommand.Flag("min-age", "Minimum age of storage block to delete").Default("24h").Duration()
	storageSweepPrintUnused  = storageSweepCommand.Flag("print", "Do not delete blocks, but print which ones would be deleted").Short('n').Bool()
)

func runStorageSweepAction(ctx context.Context, rep *repo.Repository) error {
	indexBlocks, err := rep.Blocks.ActiveIndexBlocks(ctx)
	if err != nil {
		return err
	}

	inUseIndexBlocks := map[block.PhysicalBlockID]bool{}
	for _, ib := range indexBlocks {
		inUseIndexBlocks[ib.BlockID] = true
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := rep.Storage.ListBlocks(ctx, "")

	for bm := range ch {
		if bm.Error != nil {
			return bm.Error
		}

		if err = sweepBlock(ctx, rep, bm, inUseIndexBlocks); err != nil {
			return err
		}
	}

	return nil
}

func sweepBlock(ctx context.Context, rep *repo.Repository, bm storage.BlockMetadata, inUseIndexBlocks map[block.PhysicalBlockID]bool) error {
	age := time.Since(bm.TimeStamp)
	inUse := rep.Blocks.IsStorageBlockInUse(block.PhysicalBlockID(bm.BlockID)) || inUseIndexBlocks[block.PhysicalBlockID(bm.BlockID)] || bm.BlockID == repo.FormatBlockID
	keep := inUse || age < *storageSweepAgeThreshold
	if keep {
		return nil
	}

	if *storageSweepPrintUnused {
		fmt.Printf("unused block %v age %v\n", bm.BlockID, age)
		return nil
	}

	fmt.Printf("deleting unused block %v age %v\n", bm.BlockID, age)
	return rep.Storage.DeleteBlock(ctx, bm.BlockID)
}

func init() {
	storageSweepCommand.Action(repositoryAction(runStorageSweepAction))
}
