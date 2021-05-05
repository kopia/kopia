package cli

import (
	"context"
	"strconv"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

type commandContentStats struct {
	raw          bool
	contentRange contentRangeFlags
	out          textOutput
}

func (c *commandContentStats) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("stats", "Content statistics")
	cmd.Flag("raw", "Raw numbers").Short('r').BoolVar(&c.raw)
	c.contentRange.setup(cmd)
	c.out.setup(svc)
	cmd.Action(svc.directRepositoryReadAction(c.run))
}

func (c *commandContentStats) run(ctx context.Context, rep repo.DirectRepository) error {
	var sizeThreshold uint32 = 10

	countMap := map[uint32]int{}
	totalSizeOfContentsUnder := map[uint32]int64{}

	var sizeThresholds []uint32

	for i := 0; i < 8; i++ {
		sizeThresholds = append(sizeThresholds, sizeThreshold)
		countMap[sizeThreshold] = 0
		sizeThreshold *= 10
	}

	var totalSize, count int64

	if err := rep.ContentReader().IterateContents(
		ctx,
		content.IterateOptions{
			Range: c.contentRange.contentIDRange(),
		},
		func(b content.Info) error {
			totalSize += int64(b.GetPackedLength())
			count++
			for s := range countMap {
				if b.GetPackedLength() < s {
					countMap[s]++
					totalSizeOfContentsUnder[s] += int64(b.GetPackedLength())
				}
			}
			return nil
		}); err != nil {
		return errors.Wrap(err, "error iterating contents")
	}

	sizeToString := units.BytesStringBase10
	if c.raw {
		sizeToString = func(l int64) string { return strconv.FormatInt(l, 10) }
	}

	c.out.printStdout("Count: %v\n", count)
	c.out.printStdout("Total: %v\n", sizeToString(totalSize))

	if count == 0 {
		return nil
	}

	c.out.printStdout("Average: %v\n", sizeToString(totalSize/count))

	c.out.printStdout("Histogram:\n\n")

	var lastSize uint32

	for _, size := range sizeThresholds {
		c.out.printStdout("%9v between %v and %v (total %v)\n",
			countMap[size]-countMap[lastSize],
			sizeToString(int64(lastSize)),
			sizeToString(int64(size)),
			sizeToString(totalSizeOfContentsUnder[size]-totalSizeOfContentsUnder[lastSize]),
		)

		lastSize = size
	}

	return nil
}
