package cli

import (
	"context"
	"fmt"
	"sort"

	"github.com/kopia/kopia/repo"
)

var (
	blockIndexListCommand = indexCommands.Command("list", "List content indexes").Alias("ls").Default()
	blockIndexListSummary = blockIndexListCommand.Flag("summary", "Display index blob summary").Bool()
	blockIndexListSort    = blockIndexListCommand.Flag("sort", "Index blob sort order").Default("time").Enum("time", "size", "name")
)

func runListBlockIndexesAction(ctx context.Context, rep *repo.DirectRepository) error {
	blks, err := rep.Content.IndexBlobs(ctx)
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
			return blks[i].BlobID < blks[j].BlobID
		})
	}

	for _, b := range blks {
		fmt.Printf("%-70v %10v %v\n", b.BlobID, b.Length, formatTimestampPrecise(b.Timestamp))
	}

	if *blockIndexListSummary {
		fmt.Printf("total %v blocks\n", len(blks))
	}

	return nil
}

func init() {
	blockIndexListCommand.Action(directRepositoryAction(runListBlockIndexesAction))
}
