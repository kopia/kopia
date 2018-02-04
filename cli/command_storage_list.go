package cli

import (
	"fmt"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	storageListCommand = storageCommands.Command("list", "List storage blocks").Alias("ls")
	storageListPrefix  = storageListCommand.Flag("prefix", "Block prefix").String()
)

func runListStorageBlocks(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	ch, cancel := rep.Storage.ListBlocks(*storageListPrefix)
	defer cancel()

	for b := range ch {
		if b.Error != nil {
			return b.Error
		}

		fmt.Printf("%-50v %10v %v\n", b.BlockID, b.Length, b.TimeStamp.Local().Format(timeFormat))
	}

	return nil
}

func init() {
	storageListCommand.Action(runListStorageBlocks)
}
