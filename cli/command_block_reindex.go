package cli

import (
	"time"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	optimizeCommand = blockCommands.Command("reindex", "Optimize block indexes.")
	optimizeMinAge  = optimizeCommand.Flag("min-age", "Minimum age of blocks to re-index").Default("24h").Duration()
)

func runOptimizeCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	return rep.Blocks.CompactIndexes(time.Now().Add(-*optimizeMinAge), nil)
}

func init() {
	optimizeCommand.Action(runOptimizeCommand)
}
