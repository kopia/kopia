package cli

import "gopkg.in/alecthomas/kingpin.v2"

var (
	metadataRemoveCommand = metadataCommands.Command("rm", "Remove metadata items").Hidden()
	metadataRemoveItems   = metadataRemoveCommand.Arg("item", "Items to remove").Strings()
)

func init() {
	metadataRemoveCommand.Action(removeMetadataItem)
}

func removeMetadataItem(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)

	return rep.RemoveMany(*metadataRemoveItems)
}
