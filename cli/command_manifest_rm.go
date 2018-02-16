package cli

import "gopkg.in/alecthomas/kingpin.v2"

var (
	manifestRemoveCommand = manifestCommands.Command("rm", "Remove manifest items")
	manifestRemoveItems   = manifestRemoveCommand.Arg("item", "Items to remove").Required().Strings()
)

func init() {
	manifestRemoveCommand.Action(removeMetadataItem)
}

func removeMetadataItem(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)

	for _, it := range *manifestRemoveItems {
		rep.Manifests.Delete(it)
	}

	return nil
}
