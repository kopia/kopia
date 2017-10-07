package cli

import (
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	blockRepackCommand       = blockCommands.Command("repack", "Repackage small blocks into bigger ones")
	blockRepackSizeThreshold = blockRepackCommand.Flag("threshold", "Min size of block to re-pack").Default("100000").Int64()
)

func runBlockRepackAction(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	blocks := rep.Blocks.ListBlocks("", "packs")
	_ = blocks

	return nil
}

func init() {
	blockRepackCommand.Action(runBlockRepackAction)
}
