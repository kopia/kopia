package cli

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

var (
	blobListCommand = blobCommands.Command("list", "List BLOBs").Alias("ls")
	blobListPrefix  = blobListCommand.Flag("prefix", "Blob ID prefix").String()
	blobListMinSize = blobListCommand.Flag("min-size", "Minimum size").Int64()
	blobListMaxSize = blobListCommand.Flag("max-size", "Maximum size").Int64()
)

func runBlobList(ctx context.Context, rep *repo.DirectRepository) error {
	return rep.Blobs.ListBlobs(ctx, blob.ID(*blobListPrefix), func(b blob.Metadata) error {
		if *blobListMaxSize != 0 && b.Length > *blobListMaxSize {
			return nil
		}

		if *blobListMinSize != 0 && b.Length < *blobListMinSize {
			return nil
		}

		fmt.Printf("%-70v %10v %v\n", b.BlobID, b.Length, formatTimestamp(b.Timestamp))
		return nil
	})
}

func init() {
	blobListCommand.Action(directRepositoryAction(runBlobList))
}
