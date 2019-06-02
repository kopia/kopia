package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

var (
	contentGarbageCollectCommand       = contentCommands.Command("gc", "Garbage-collect unused blobs")
	contentGarbageCollectCommandDelete = contentGarbageCollectCommand.Flag("delete", "Whether to delete unused blobs").String()
)

func runContentGarbageCollectCommand(ctx context.Context, rep *repo.Repository) error {
	unused, err := rep.Content.FindUnreferencedBlobs(ctx)
	if err != nil {
		return errors.Wrap(err, "error looking for unreferenced blobs")
	}

	if len(unused) == 0 {
		printStderr("No unused blobs found.\n")
		return nil
	}

	if *contentGarbageCollectCommandDelete != "yes" {
		var totalBytes int64
		for _, u := range unused {
			printStderr("unused %v (%v bytes)\n", u.BlobID, u.Length)
			totalBytes += u.Length
		}
		printStderr("Would delete %v unused blobs (%v bytes), pass '--delete=yes' to actually delete.\n", len(unused), totalBytes)

		return nil
	}

	for _, u := range unused {
		printStderr("Deleting unused blob %q (%v bytes)...\n", u.BlobID, u.Length)
		if err := rep.Blobs.DeleteBlob(ctx, u.BlobID); err != nil {
			return errors.Wrapf(err, "unable to delete blob %q", u.BlobID)
		}
	}

	return nil
}

func init() {
	contentGarbageCollectCommand.Action(repositoryAction(runContentGarbageCollectCommand))
}
