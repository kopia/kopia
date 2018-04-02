package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

var (
	optimizeCommand = blockIndexCommands.Command("optimize", "Optimize block indexes.")
)

func runOptimizeCommand(ctx context.Context, rep *repo.Repository) error {
	return rep.Blocks.CompactIndexes(ctx)
}

func init() {
	optimizeCommand.Action(repositoryAction(runOptimizeCommand))
}
