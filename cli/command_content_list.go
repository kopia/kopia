package cli

import (
	"context"
	"fmt"
	"sort"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

var (
	contentListCommand        = contentCommands.Command("list", "List contents").Alias("ls")
	contentListLong           = contentListCommand.Flag("long", "Long output").Short('l').Bool()
	contentListPrefix         = contentListCommand.Flag("prefix", "Prefix").String()
	contentListIncludeDeleted = contentListCommand.Flag("deleted", "Include deleted content").Bool()
	contentListDeletedOnly    = contentListCommand.Flag("deleted-only", "Only show deleted content").Bool()
	contentListSort           = contentListCommand.Flag("sort", "Sort order").Default("name").Enum("name", "size", "time", "none", "pack")
	contentListReverse        = contentListCommand.Flag("reverse", "Reverse sort").Short('r').Bool()
	contentListSummary        = contentListCommand.Flag("summary", "Summarize the list").Short('s').Bool()
	contentListHuman          = contentListCommand.Flag("human", "Human-readable output").Short('h').Bool()
)

func runContentListCommand(ctx context.Context, rep *repo.Repository) error {
	contents, err := rep.Content.ListContentInfos(content.ID(*contentListPrefix), *contentListIncludeDeleted || *contentListDeletedOnly)
	if err != nil {
		return err
	}

	sortContents(contents)

	var count int
	var totalSize int64
	uniquePacks := map[blob.ID]bool{}
	for _, b := range contents {
		if *contentListDeletedOnly && !b.Deleted {
			continue
		}
		totalSize += int64(b.Length)
		count++
		if b.PackBlobID != "" {
			uniquePacks[b.PackBlobID] = true
		}
		if *contentListLong {
			optionalDeleted := ""
			if b.Deleted {
				optionalDeleted = " (deleted)"
			}
			fmt.Printf("%v %v %v %v+%v%v\n",
				b.ID,
				formatTimestamp(b.Timestamp()),
				b.PackBlobID,
				b.PackOffset,
				maybeHumanReadableBytes(*contentListHuman, int64(b.Length)),
				optionalDeleted)
		} else {
			fmt.Printf("%v\n", b.ID)
		}
	}

	if *contentListSummary {
		fmt.Printf("Total: %v contents, %v packs, %v total size\n",
			maybeHumanReadableCount(*contentListHuman, int64(count)),
			maybeHumanReadableCount(*contentListHuman, int64(len(uniquePacks))),
			maybeHumanReadableBytes(*contentListHuman, totalSize))
	}

	return nil
}

func sortContents(contents []content.Info) {
	maybeReverse := func(b bool) bool { return b }

	if *contentListReverse {
		maybeReverse = func(b bool) bool { return !b }
	}

	switch *contentListSort {
	case "name":
		sort.Slice(contents, func(i, j int) bool { return maybeReverse(contents[i].ID < contents[j].ID) })
	case "size":
		sort.Slice(contents, func(i, j int) bool { return maybeReverse(contents[i].Length < contents[j].Length) })
	case "time":
		sort.Slice(contents, func(i, j int) bool { return maybeReverse(contents[i].TimestampSeconds < contents[j].TimestampSeconds) })
	case "pack":
		sort.Slice(contents, func(i, j int) bool { return maybeReverse(comparePacks(contents[i], contents[j])) })
	}
}

func comparePacks(a, b content.Info) bool {
	if a, b := a.PackBlobID, b.PackBlobID; a != b {
		return a < b
	}

	return a.PackOffset < b.PackOffset
}

func init() {
	contentListCommand.Action(repositoryAction(runContentListCommand))
}
