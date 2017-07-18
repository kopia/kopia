package cli

import (
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	metadataListCommand = metadataCommands.Command("list", "List metadata items").Alias("ls").Hidden()
	metadataListPrefix  = metadataListCommand.Flag("prefix", "Prefix").String()
)

func init() {
	metadataListCommand.Action(listMetadataItems)
}

func listMetadataItems(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)

	entries, err := rep.ListMetadata(*metadataListPrefix, -1)
	if err != nil {
		return err
	}

	for _, e := range entries {
		fmt.Println(e)
	}

	return nil
}
