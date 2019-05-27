package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/pkg/errors"
)

var (
	blockGarbageCollectCommand       = blockCommands.Command("gc", "Garbage-collect unused storage blocks")
	blockGarbageCollectCommandDelete = blockGarbageCollectCommand.Flag("delete", "Whether to delete unused block").String()
)

func runBlockGarbageCollectAction(ctx context.Context, rep *repo.Repository) error {
	unused, err := rep.Blocks.FindUnreferencedStorageFiles(ctx)
	if err != nil {
		return errors.Wrap(err, "error looking for unreferenced storage files")
	}

	if len(unused) == 0 {
		printStderr("No unused blocks found.\n")
		return nil
	}

	if *blockGarbageCollectCommandDelete != "yes" {
		var totalBytes int64
		for _, u := range unused {
			printStderr("unused %v (%v bytes)\n", u.BlockID, u.Length)
			totalBytes += u.Length
		}
		printStderr("Would delete %v unused blocks (%v bytes), pass '--delete=yes' to actually delete.\n", len(unused), totalBytes)

		return nil
	}

	for _, u := range unused {
		printStderr("Deleting unused block %q (%v bytes)...\n", u.BlockID, u.Length)
		if err := rep.Storage.DeleteBlock(ctx, u.BlockID); err != nil {
			return errors.Wrapf(err, "unable to delete block %q", u.BlockID)
		}
	}

	return nil
}

func init() {
	blockGarbageCollectCommand.Action(repositoryAction(runBlockGarbageCollectAction))
}
