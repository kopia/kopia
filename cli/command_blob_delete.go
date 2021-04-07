package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

var (
	blobDeleteCommand = blobCommands.Command("delete", "Delete blobs by ID").Alias("remove").Alias("rm")
	blobDeleteBlobIDs = blobDeleteCommand.Arg("blobIDs", "Blob IDs").Required().Strings()
)

func runDeleteBlobs(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	advancedCommand(ctx)

	for _, b := range *blobDeleteBlobIDs {
		err := rep.BlobStorage().DeleteBlob(ctx, blob.ID(b))
		if err != nil {
			return errors.Wrapf(err, "error deleting %v", b)
		}
	}

	return nil
}

func init() {
	blobDeleteCommand.Action(directRepositoryWriteAction(runDeleteBlobs))
}
