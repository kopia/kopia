package cli

import (
	"time"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	optimizeCommand = repositoryCommands.Command("optimize", "Optimize repository performance.")
	optimizeMinAge  = optimizeCommand.Flag("min-age", "Minimum age of objects to optimize").Default("24h").Duration()
)

func runOptimizeCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	return rep.Optimize(time.Now().Add(-*optimizeMinAge))
}

func init() {
	optimizeCommand.Action(runOptimizeCommand)
}
