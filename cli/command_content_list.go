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
	contentListIncludeDeleted = contentListCommand.Flag("deleted", "Include deleted content").Bool()
	contentListDeletedOnly    = contentListCommand.Flag("deleted-only", "Only show deleted content").Bool()
	contentListSummary        = contentListCommand.Flag("summary", "Summarize the list").Short('s').Bool()
	contentListHuman          = contentListCommand.Flag("human", "Human-readable output").Short('h').Bool()
)

func runContentListCommand(ctx context.Context, rep repo.DirectRepository) error {
	var jl jsonList

	jl.begin()
	defer jl.end()

	var totalSize stats.CountSum

	err := rep.ContentReader().IterateContents(
		ctx,
		content.IterateOptions{
			Range:          contentIDRange(),
			IncludeDeleted: *contentListIncludeDeleted || *contentListDeletedOnly,
		},
		func(b content.Info) error {
			if *contentListDeletedOnly && !b.GetDeleted() {
				return nil
			}

			totalSize.Add(int64(b.GetPackedLength()))

			if jsonOutput {
				jl.emit(b)
				return nil
			}

			if *contentListLong {
				optionalDeleted := ""
				if b.GetDeleted() {
					optionalDeleted = " (deleted)"
				}
				fmt.Printf("%v %v %v %v+%v%v\n",
					b.GetContentID(),
					formatTimestamp(b.Timestamp()),
					b.GetPackBlobID(),
					b.GetPackOffset(),
					maybeHumanReadableBytes(*contentListHuman, int64(b.GetPackedLength())),
					optionalDeleted)
			} else {
				fmt.Printf("%v\n", b.GetContentID())
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
	registerJSONOutputFlags(contentListCommand)
	contentListCommand.Action(directRepositoryReadAction(runContentListCommand))
	setupContentIDRangeFlags(contentListCommand)
}
