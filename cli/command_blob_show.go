package cli

import (
	"bytes"
	"context"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

var (
	blobShowCommand = blobCommands.Command("show", "Show contents of BLOBs").Alias("cat")
	blobShowIDs     = blobShowCommand.Arg("blobID", "Blob IDs").Required().Strings()
)

func runBlobShow(ctx context.Context, rep *repo.DirectRepository) error {
	for _, blobID := range *blobShowIDs {
		d, err := rep.Blobs.GetBlob(ctx, blob.ID(blobID), 0, -1)
		if err != nil {
			return errors.Wrapf(err, "error getting %v", blobID)
		}

		if _, err := iocopy.Copy(os.Stdout, bytes.NewReader(d)); err != nil {
			return err
		}
	}

	return nil
}

func init() {
	blobShowCommand.Action(directRepositoryAction(runBlobShow))
}
