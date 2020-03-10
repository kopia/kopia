package cli

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/stats"
	"github.com/kopia/kopia/repo"
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

func runContentListCommand(ctx context.Context, rep *repo.DirectRepository) error {
	var totalSize stats.CountSum

	err := rep.Content.IterateContents(
		ctx,
		content.IterateOptions{
			Prefix:         content.ID(*contentListPrefix),
			IncludeDeleted: *contentListIncludeDeleted || *contentListDeletedOnly,
		},
		func(b content.Info) error {
			if *contentListDeletedOnly && !b.Deleted {
				return nil
			}

			totalSize.Add(int64(b.Length))

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
		count, sz := totalSize.Approximate()
		fmt.Printf("Total: %v contents, %v total size\n",
			maybeHumanReadableCount(*contentListHuman, int64(count)),
			maybeHumanReadableBytes(*contentListHuman, sz))
	}

	return nil
}

func init() {
	contentListCommand.Action(directRepositoryAction(runContentListCommand))
}
