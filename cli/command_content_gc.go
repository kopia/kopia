package cli

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

var (
	contentGarbageCollectCommand       = contentCommands.Command("gc", "Garbage-collect unused blobs")
	contentGarbageCollectCommandDelete = contentGarbageCollectCommand.Flag("delete", "Whether to delete unused blobs").String()
	contentGarbageCollectParallel      = contentGarbageCollectCommand.Flag("parallel", "Number of parallel blob scans").Int()
)

func runContentGarbageCollectCommand(ctx context.Context, rep *repo.Repository) error {
	var mu sync.Mutex
	var unused []blob.Metadata

	if err := rep.Content.IterateUnreferencedBlobs(ctx, *contentGarbageCollectParallel, func(bm blob.Metadata) error {
		mu.Lock()
		unused = append(unused, bm)
		mu.Unlock()
		return nil
	}); err != nil {
		return errors.Wrap(err, "error looking for unreferenced blobs")
	}

	if len(unused) == 0 {
		printStderr("No unused blobs found.\n")
		return nil
	}

	if *contentGarbageCollectCommandDelete != "yes" {
		var totalBytes int64
		for _, u := range unused {
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
