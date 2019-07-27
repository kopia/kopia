package cli

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

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
	contentListSummary        = contentListCommand.Flag("summary", "Summarize the list").Short('s').Bool()
	contentListHuman          = contentListCommand.Flag("human", "Human-readable output").Short('h').Bool()
)

func runContentListCommand(ctx context.Context, rep *repo.Repository) error {
	var count int
	var totalSize int64
	uniquePacks := map[blob.ID]bool{}
	err := rep.Content.IterateContents(content.ID(*contentListPrefix), *contentListIncludeDeleted || *contentListDeletedOnly, func(b content.Info) error {
		if *contentListDeletedOnly && !b.Deleted {
			return nil
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

		return nil
	})

	if err != nil {
		return errors.Wrap(err, "error iterating")
	}

	if *contentListSummary {
		fmt.Printf("Total: %v contents, %v packs, %v total size\n",
			maybeHumanReadableCount(*contentListHuman, int64(count)),
			maybeHumanReadableCount(*contentListHuman, int64(len(uniquePacks))),
			maybeHumanReadableBytes(*contentListHuman, totalSize))
	}

	return nil
}

func init() {
	contentListCommand.Action(repositoryAction(runContentListCommand))
}
