package cli

import (
	"fmt"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	blockIndexListCommand = blockIndexCommands.Command("list", "List block indexes").Alias("ls")
)

func runListBlockIndexesAction(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	ch, cancel := rep.Storage.ListBlocks("P")
	defer cancel()

	for b := range ch {
		fmt.Printf("%-34v %10v %v\n", b.BlockID, b.Length, b.TimeStamp.Local().Format(timeFormat))
	}

	return nil
}

func init() {
	blockIndexListCommand.Action(runListBlockIndexesAction)
}
