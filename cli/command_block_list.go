package cli

import (
	"context"
	"fmt"
	"sort"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/block"
)

var (
	blockListCommand        = blockCommands.Command("list", "List blocks").Alias("ls")
	blockListLong           = blockListCommand.Flag("long", "Long output").Short('l').Bool()
	blockListPrefix         = blockListCommand.Flag("prefix", "Prefix").String()
	blockListIncludeDeleted = blockListCommand.Flag("deleted", "Include deleted blocks").Bool()
	blockListDeletedOnly    = blockListCommand.Flag("deleted-only", "Only show deleted blocks").Bool()
	blockListSort           = blockListCommand.Flag("sort", "Sort order").Default("name").Enum("name", "size", "time", "none", "pack")
	blockListReverse        = blockListCommand.Flag("reverse", "Reverse sort").Short('r').Bool()
	blockListSummary        = blockListCommand.Flag("summary", "Summarize the list").Short('s').Bool()
	blockListHuman          = blockListCommand.Flag("human", "Human-readable output").Short('h').Bool()
)

func runListBlocksAction(ctx context.Context, rep *repo.Repository) error {
	blocks, err := rep.Blocks.ListBlockInfos(*blockListPrefix, *blockListIncludeDeleted || *blockListDeletedOnly)
	if err != nil {
		return err
	}

	sortBlocks(blocks)

	var count int
	var totalSize int64
	uniquePacks := map[string]bool{}
	for _, b := range blocks {
		if *blockListDeletedOnly && !b.Deleted {
			continue
		}
		totalSize += int64(b.Length)
		count++
		if b.PackFile != "" {
			uniquePacks[b.PackFile] = true
		}
		if *blockListLong {
			optionalDeleted := ""
			if b.Deleted {
				optionalDeleted = " (deleted)"
			}
			fmt.Printf("%v %v %v %v+%v%v\n",
				b.BlockID,
				b.Timestamp().Format(timeFormat),
				b.PackFile,
				b.PackOffset,
				maybeHumanReadableBytes(*blockListHuman, int64(b.Length)),
				optionalDeleted)
		} else {
			fmt.Printf("%v\n", b.BlockID)
		}
	}

	if *blockListSummary {
		fmt.Printf("Total: %v blocks, %v packs, %v total size\n",
			maybeHumanReadableCount(*blockListHuman, int64(count)),
			maybeHumanReadableCount(*blockListHuman, int64(len(uniquePacks))),
			maybeHumanReadableBytes(*blockListHuman, totalSize))
	}

	return nil
}

func sortBlocks(blocks []block.Info) {
	maybeReverse := func(b bool) bool { return b }

	if *blockListReverse {
		maybeReverse = func(b bool) bool { return !b }
	}

	switch *blockListSort {
	case "name":
		sort.Slice(blocks, func(i, j int) bool { return maybeReverse(blocks[i].BlockID < blocks[j].BlockID) })
	case "size":
		sort.Slice(blocks, func(i, j int) bool { return maybeReverse(blocks[i].Length < blocks[j].Length) })
	case "time":
		sort.Slice(blocks, func(i, j int) bool { return maybeReverse(blocks[i].TimestampSeconds < blocks[j].TimestampSeconds) })
	case "pack":
		sort.Slice(blocks, func(i, j int) bool { return maybeReverse(comparePacks(blocks[i], blocks[j])) })
	}
}

func comparePacks(a, b block.Info) bool {
	if a, b := a.PackFile, b.PackFile; a != b {
		return a < b
	}

	return a.PackOffset < b.PackOffset
}

func init() {
	blockListCommand.Action(repositoryAction(runListBlocksAction))
}
