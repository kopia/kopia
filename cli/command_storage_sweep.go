package cli

import (
	"fmt"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/storage"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	storageSweepCommand      = storageCommands.Command("sweep", "Remove unused storage blocks")
	storageSweepAgeThreshold = storageSweepCommand.Flag("min-age", "Minimum age of storage block to delete").Default("24h").Duration()
	storageSweepPrintUnused  = storageSweepCommand.Flag("print", "Do not delete blocks, but print which ones would be deleted").Short('n').Bool()
)

func runStorageSweepAction(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close() //nolint: errcheck

	indexBlocks, err := rep.Blocks.ActiveIndexBlocks()
	if err != nil {
		return err
	}

	inUseIndexBlocks := map[string]bool{}
	for _, ib := range indexBlocks {
		inUseIndexBlocks[ib.BlockID] = true
	}

	ch, cancel := rep.Storage.ListBlocks("")
	defer cancel()

	for bm := range ch {
		if bm.Error != nil {
			return bm.Error
		}

		if err = sweepBlock(rep, bm, inUseIndexBlocks); err != nil {
			return err
		}
	}

	return nil
}

func sweepBlock(rep *repo.Repository, bm storage.BlockMetadata, inUseIndexBlocks map[string]bool) error {
	age := time.Since(bm.TimeStamp)
	inUse := rep.Blocks.IsStorageBlockInUse(bm.BlockID) || inUseIndexBlocks[bm.BlockID] || bm.BlockID == repo.FormatBlockID
	keep := inUse || age < *storageSweepAgeThreshold
	if keep {
		return nil
	}

	if *storageSweepPrintUnused {
		fmt.Printf("unused block %v age %v\n", bm.BlockID, age)
		return nil
	}

	fmt.Printf("deleting unused block %v age %v\n", bm.BlockID, age)
	return rep.Storage.DeleteBlock(bm.BlockID)
}

func init() {
	storageSweepCommand.Action(runStorageSweepAction)
}
