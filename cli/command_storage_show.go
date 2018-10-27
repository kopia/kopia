package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/kopia/repo"
)

var (
	storageShowCommand  = storageCommands.Command("show", "Show storage blocks").Alias("cat")
	storageShowBlockIDs = storageShowCommand.Arg("blockIDs", "Block IDs").Required().Strings()
)

func runShowStorageBlocks(ctx context.Context, rep *repo.Repository) error {
	for _, b := range *storageShowBlockIDs {
		d, err := rep.Storage.GetBlock(ctx, b, 0, -1)
		if err != nil {
			return fmt.Errorf("error getting %v: %v", b, err)
		}
		if _, err := io.Copy(os.Stdout, bytes.NewReader(d)); err != nil {
			return err
		}
	}

	return nil
}

func init() {
	storageShowCommand.Action(repositoryAction(runShowStorageBlocks))
}
