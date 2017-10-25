package cli

import (
	"bytes"
	"fmt"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	blockIndexShowCommand = blockIndexCommands.Command("show", "List block indexes").Alias("cat")
	blockIndexShowIDs     = blockIndexShowCommand.Arg("id", "IDs of index blocks to show").Required().Strings()
)

func runShowBlockIndexesAction(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	for _, blockID := range *blockIndexShowIDs {
		data, err := rep.Blocks.GetBlock(blockID)
		if err != nil {
			return fmt.Errorf("can't read block %q: %v", blockID, err)
		}

		if err := showContentWithFlags(bytes.NewReader(data), true, true); err != nil {
			return err
		}
	}

	return nil
}

func init() {
	blockIndexShowCommand.Action(runShowBlockIndexesAction)
}
