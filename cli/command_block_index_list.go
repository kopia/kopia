package cli

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/repo"
)

var (
	blockIndexListCommand = blockIndexCommands.Command("list", "List block indexes").Alias("ls").Default()
	blockIndexListSummary = blockIndexListCommand.Flag("summary", "Display block summary").Bool()
)

func runListBlockIndexesAction(ctx context.Context, rep *repo.Repository) error {
	blks, err := rep.Blocks.IndexBlocks(ctx)

	if err != nil {
		return err
	}

	for _, b := range blks {
		fmt.Printf("%-70v %10v %v\n", b.FileName, b.Length, formatTimestampPrecise(b.Timestamp))
	}

	if *blockIndexListSummary {
		fmt.Printf("total %v blocks\n", len(blks))
	}

	return nil
}

func init() {
	blockIndexListCommand.Action(repositoryAction(runListBlockIndexesAction))
}
