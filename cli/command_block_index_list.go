package cli

import (
	"context"
	"fmt"
	"sort"

	"github.com/kopia/repo"
)

var (
	blockIndexListCommand = blockIndexCommands.Command("list", "List block indexes").Alias("ls").Default()
	blockIndexListSummary = blockIndexListCommand.Flag("summary", "Display block summary").Bool()
	blockIndexListSort    = blockIndexListCommand.Flag("sort", "Index block sort order").Default("time").Enum("time", "size", "name")
)

func runListBlockIndexesAction(ctx context.Context, rep *repo.Repository) error {
	blks, err := rep.Blocks.IndexBlocks(ctx)
	if err != nil {
		return err
	}

	switch *blockIndexListSort {
	case "time":
		sort.Slice(blks, func(i, j int) bool {
			return blks[i].Timestamp.Before(blks[j].Timestamp)
		})
	case "size":
		sort.Slice(blks, func(i, j int) bool {
			return blks[i].Length < blks[j].Length
		})
	case "name":
		sort.Slice(blks, func(i, j int) bool {
			return blks[i].FileName < blks[j].FileName
		})
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
