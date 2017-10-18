package cli

import (
	"fmt"
	"sort"

	"github.com/kopia/kopia/block"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	blockListCommand = blockCommands.Command("list", "List blocks").Alias("ls")
	blockListKind    = blockListCommand.Flag("kind", "Block kind").Default("all").Enum("all", "physical", "packed", "nonpacked", "packs")
	blockListGroup   = blockListCommand.Flag("group", "List blocks belonging to a given group").String()
	blockListLong    = blockListCommand.Flag("long", "Long output").Short('l').Bool()
	blockListPrefix  = blockListCommand.Flag("prefix", "Prefix").String()
	blockListSort    = blockListCommand.Flag("sort", "Sort order").Default("name").Enum("name", "size", "time", "none", "pack")
	blockListReverse = blockListCommand.Flag("reverse", "Reverse sort").Short('r').Bool()
	blockListSummary = blockListCommand.Flag("summary", "Summarize the list").Short('s').Bool()
)

func runListBlocksAction(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	var blocks []block.Info
	if *blockListGroup != "" {
		blocks = rep.Blocks.ListGroupBlocks(*blockListGroup)
	} else {
		blocks = rep.Blocks.ListBlocks(*blockListPrefix, *blockListKind)
	}
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
		sort.Slice(blocks, func(i, j int) bool { return maybeReverse(blocks[i].Timestamp.Before(blocks[j].Timestamp)) })
	case "pack":
		sort.Slice(blocks, func(i, j int) bool { return maybeReverse(comparePacks(blocks[i], blocks[j])) })
	}

	var count int
	var totalSize int64
	uniquePacks := map[string]bool{}
	for _, b := range blocks {
		totalSize += b.Length
		count++
		if b.PackBlockID != "" {
			uniquePacks[b.PackBlockID] = true
		}
		if *blockListLong {
			grp := b.PackGroup
			if grp == "" {
				grp = "default"
			}
			if b.PackBlockID != "" {
				fmt.Printf("%-34v %10v %v %v in %v offset %v\n", b.BlockID, b.Length, b.Timestamp.Local().Format(timeFormat), grp, b.PackBlockID, b.PackOffset)
			} else {
				fmt.Printf("%-34v %10v %v %v\n", b.BlockID, b.Length, b.Timestamp.Local().Format(timeFormat), grp)
			}
		} else {
			fmt.Printf("%v\n", b.BlockID)
		}
	}

	if *blockListSummary {
		fmt.Printf("Total: %v blocks, %v packs, %v bytes\n", count, len(uniquePacks), totalSize)
	}

	return nil
}

func comparePacks(a, b block.Info) bool {
	if a, b := a.PackBlockID, b.PackBlockID; a != b {
		return a < b
	}

	return a.PackOffset < b.PackOffset
}

func init() {
	blockListCommand.Action(runListBlocksAction)
}
