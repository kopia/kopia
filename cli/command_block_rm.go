package cli

import (
	"github.com/kopia/kopia/repo"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	removeBlockCommand = blockCommands.Command("remove", "Remove block(s)").Alias("rm")

	removeBlockIDs = removeBlockCommand.Arg("id", "IDs of blocks to remove").Required().Strings()
)

func runRemoveBlockCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close() //nolint: errcheck

	for _, blockID := range *removeBlockIDs {
		if err := removeBlock(rep, blockID); err != nil {
			return err
		}
	}

	return nil
}

func removeBlock(r *repo.Repository, blockID string) error {
	return r.Blocks.DeleteBlock(blockID)
}

func init() {
	setupShowCommand(removeBlockCommand)
	removeBlockCommand.Action(runRemoveBlockCommand)
}
