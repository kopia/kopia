package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

var (
	blobDeleteCommand = blobCommands.Command("delete", "Show contents of blobs").Alias("rm")
	blobDeleteBlobIDs = blobDeleteCommand.Arg("blobIDs", "Blob IDs").Required().Strings()
)

func runDeleteStorageBlocks(ctx context.Context, rep *repo.Repository) error {
	for _, b := range *blobDeleteBlobIDs {
		err := rep.Blobs.DeleteBlob(ctx, blob.ID(b))
		if err != nil {
			return errors.Wrapf(err, "error deleting %v", b)
		}
	}

	return nil
}

func init() {
	blobDeleteCommand.Action(repositoryAction(runDeleteStorageBlocks))
}
