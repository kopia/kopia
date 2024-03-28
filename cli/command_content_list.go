package cli

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/stats"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
)

type commandContentList struct {
	long           bool
	includeDeleted bool
	deletedOnly    bool
	summary        bool
	human          bool
	compression    bool

	contentRange contentRangeFlags
	jo           jsonOutput
	out          textOutput
}

func (c *commandContentList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List contents").Alias("ls")
	cmd.Flag("long", "Long output").Short('l').BoolVar(&c.long)
	cmd.Flag("compression", "Compression").Short('c').BoolVar(&c.compression)
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
			if c.deletedOnly && !b.Deleted {
				return nil
			}

			totalSize.Add(int64(b.PackedLength))

			switch {
			case c.jo.jsonOutput:
				jl.emit(b)
			case c.compression:
				c.outputCompressed(b)
			case c.long:
				c.outputLong(b)
			default:
				c.out.printStdout("%v\n", b.ContentID)
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

func (c *commandContentList) outputLong(b content.Info) {
	c.out.printStdout("%v %v %v %v %v+%v%v %v\n",
		b.ContentID,
		b.OriginalLength,
		formatTimestamp(b.Timestamp()),
		b.PackBlobID,
		b.PackOffset,
		maybeHumanReadableBytes(c.human, int64(b.PackedLength)),
		c.deletedInfoString(b),
		c.compressionInfoStringString(b),
	)
}

func (c *commandContentList) outputCompressed(b content.Info) {
	c.out.printStdout("%v length %v packed %v %v %v\n",
		b.ContentID,
		maybeHumanReadableBytes(c.human, int64(b.OriginalLength)),
		maybeHumanReadableBytes(c.human, int64(b.PackedLength)),
		c.compressionInfoStringString(b),
		c.deletedInfoString(b),
	)
}

func (*commandContentList) deletedInfoString(b content.Info) string {
	if b.Deleted {
		return " (deleted)"
	}

	return ""
}

func (*commandContentList) compressionInfoStringString(b content.Info) string {
	h := b.CompressionHeaderID
	if h == content.NoCompression {
		return "-"
	}

	s := string(compression.HeaderIDToName[h])
	if s == "" {
		s = fmt.Sprintf("compression-%x", h)
	}

	if b.OriginalLength > 0 {
		s += " " + formatCompressionPercentage(int64(b.OriginalLength), int64(b.PackedLength))
	}

	return s
}
