package cli

import (
	"context"
	"fmt"
	"sort"

	"github.com/kopia/kopia/repo"
)

var (
	blockIndexListCommand           = indexCommands.Command("list", "List content indexes").Alias("ls").Default()
	blockIndexListSummary           = blockIndexListCommand.Flag("summary", "Display index blob summary").Bool()
	blockIndexListIncludeSuperseded = blockIndexListCommand.Flag("superseded", "Include inactive index files superseded by compaction").Bool()
	blockIndexListSort              = blockIndexListCommand.Flag("sort", "Index blob sort order").Default("time").Enum("time", "size", "name")
)

func runListBlockIndexesAction(ctx context.Context, rep *repo.DirectRepository) error {
	blks, err := rep.Content.IndexBlobs(ctx, *blockIndexListIncludeSuperseded)
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
		fmt.Printf("%-40v %10v %v %v\n", b.BlobID, b.Length, formatTimestampPrecise(b.Timestamp), b.Superseded)
	}

	if *blockIndexListSummary {
		fmt.Printf("total %v indexes\n", len(blks))
	}

	return nil
}

func init() {
	blockIndexListCommand.Action(directRepositoryAction(runListBlockIndexesAction))
}
