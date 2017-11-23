package cli

import (
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	optimizeCommand = blockIndexCommands.Command("optimize", "Optimize block indexes.")
)

func runOptimizeCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	return rep.Blocks.CompactIndexes()
}

func init() {
	optimizeCommand.Action(runOptimizeCommand)
}
