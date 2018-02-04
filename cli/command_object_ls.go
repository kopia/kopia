package cli

import (
	"fmt"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	objectListCommand = objectCommands.Command("list", "List objects").Alias("ls")
	objectListPrefix  = objectListCommand.Flag("prefix", "Prefix").String()
)

func runListObjectsAction(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	info, err := rep.Blocks.ListBlocks(*objectListPrefix)
	if err != nil {
		return err
	}

	for _, b := range info {
		fmt.Printf("D%-34v %10v %v\n", b.BlockID, b.Length, b.Timestamp.Local().Format(timeFormat))
	}

	return nil
}

func init() {
	objectListCommand.Action(runListObjectsAction)
}
