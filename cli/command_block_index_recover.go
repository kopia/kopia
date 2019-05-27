package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/block"
	"github.com/kopia/kopia/repo/storage"
)

var (
	blockIndexRecoverCommand  = blockIndexCommands.Command("recover", "Recover block indexes from pack blocks")
	blockIndexRecoverPackFile = blockIndexRecoverCommand.Flag("file", "Names of pack files to recover (default=all packs)").Strings()
	blockIndexRecoverCommit   = blockIndexRecoverCommand.Flag("commit", "Commit recovered blocks").Bool()
)

func runRecoverBlockIndexesAction(ctx context.Context, rep *repo.Repository) error {
	var totalCount int

	defer func() {
		if totalCount == 0 {
			log.Noticef("No blocks recovered.")
			return
		}

		if !*blockIndexRecoverCommit {
			log.Noticef("Found %v blocks to recover, but not committed. Re-run with --commit", totalCount)
		} else {
			log.Noticef("Recovered %v blocks.", totalCount)
		}
	}()

	if len(*blockIndexRecoverPackFile) == 0 {
		return rep.Storage.ListBlocks(ctx, block.PackBlockPrefix, func(bm storage.BlockMetadata) error {
			recoverIndexFromSinglePackFile(ctx, rep, bm.BlockID, bm.Length, &totalCount)
			return nil
		})
	}

	for _, packFile := range *blockIndexRecoverPackFile {
		recoverIndexFromSinglePackFile(ctx, rep, packFile, 0, &totalCount)
	}

	return nil
}

func recoverIndexFromSinglePackFile(ctx context.Context, rep *repo.Repository, packFileName string, length int64, totalCount *int) {
	recovered, err := rep.Blocks.RecoverIndexFromPackFile(ctx, packFileName, length, *blockIndexRecoverCommit)
	if err != nil {
		log.Warningf("unable to recover index from %v: %v", packFileName, err)
		return
	}

	*totalCount += len(recovered)
	log.Infof("Recovered %v entries from %v (commit=%v)", len(recovered), packFileName, *blockIndexRecoverCommit)
}

func init() {
	blockIndexRecoverCommand.Action(repositoryAction(runRecoverBlockIndexesAction))
}
