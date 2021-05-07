package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/stats"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

type commandContentList struct {
	long           bool
	includeDeleted bool
	deletedOnly    bool
	summary        bool
	human          bool

	contentRange contentRangeFlags
	jo           jsonOutput
	out          textOutput
}

func (c *commandContentList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List contents").Alias("ls")
	cmd.Flag("long", "Long output").Short('l').BoolVar(&c.long)
	cmd.Flag("deleted", "Include deleted content").BoolVar(&c.includeDeleted)
	cmd.Flag("deleted-only", "Only show deleted content").BoolVar(&c.deletedOnly)
	cmd.Flag("summary", "Summarize the list").Short('s').BoolVar(&c.summary)
	cmd.Flag("human", "Human-readable output").Short('h').BoolVar(&c.human)
	c.contentRange.setup(cmd)
	c.jo.setup(svc, cmd)
	c.out.setup(svc)
	cmd.Action(svc.directRepositoryReadAction(c.run))
}

func (c *commandContentList) run(ctx context.Context, rep repo.DirectRepository) error {
	var jl jsonList

	jl.begin(&c.jo)
	defer jl.end()

	var totalSize stats.CountSum

	err := rep.ContentReader().IterateContents(
		ctx,
		content.IterateOptions{
			Range:          c.contentRange.contentIDRange(),
			IncludeDeleted: c.includeDeleted || c.deletedOnly,
		},
		func(b content.Info) error {
			if c.deletedOnly && !b.GetDeleted() {
				return nil
			}

			totalSize.Add(int64(b.GetPackedLength()))

			if c.jo.jsonOutput {
				jl.emit(b)
				return nil
			}

			if c.long {
				optionalDeleted := ""
				if b.GetDeleted() {
					optionalDeleted = " (deleted)"
				}
				c.out.printStdout("%v %v %v %v+%v%v\n",
					b.GetContentID(),
					formatTimestamp(b.Timestamp()),
					b.GetPackBlobID(),
					b.GetPackOffset(),
					maybeHumanReadableBytes(c.human, int64(b.GetPackedLength())),
					optionalDeleted)
			} else {
				c.out.printStdout("%v\n", b.GetContentID())
			}

			return nil
		})
	if err != nil {
		return errors.Wrap(err, "error iterating")
	}

	if c.summary {
		count, sz := totalSize.Approximate()
		c.out.printStdout("Total: %v contents, %v total size\n",
			maybeHumanReadableCount(c.human, int64(count)),
			maybeHumanReadableBytes(c.human, sz))
	}

	return nil
}
