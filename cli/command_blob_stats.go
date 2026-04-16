package cli

import (
	"context"
	"strconv"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

type commandBlobStats struct {
	raw    bool
	prefix string

	out textOutput
}

func (c *commandBlobStats) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("stats", "Blob statistics")
	cmd.Flag("raw", "Raw numbers").Short('r').BoolVar(&c.raw)
	cmd.Flag("prefix", "Blob name prefix").StringVar(&c.prefix)
	cmd.Action(svc.directRepositoryReadAction(c.run))
	c.out.setup(svc)
}

func (c *commandBlobStats) run(ctx context.Context, rep repo.DirectRepository) error {
	var sizeThreshold int64 = 10

	countMap := map[int64]int{}
	totalSizeOfContentsUnder := map[int64]int64{}

	var sizeThresholds []int64

	for range 8 {
		sizeThresholds = append(sizeThresholds, sizeThreshold)
		countMap[sizeThreshold] = 0
		sizeThreshold *= 10
	}

	var totalSize, count int64

	if err := rep.BlobReader().ListBlobs(
		ctx,
		blob.ID(c.prefix),
		func(b blob.Metadata) error {
			totalSize += b.Length
			count++
			if count%10000 == 0 {
				log(ctx).Infof("Got %v blobs...", count)
			}
			for s := range countMap {
				if b.Length < s {
					countMap[s]++
					totalSizeOfContentsUnder[s] += b.Length
				}
			}
			return nil
		}); err != nil {
		return errors.Wrap(err, "error listing blobs")
	}

	sizeToString := units.BytesString[int64]
	if c.raw {
		sizeToString = func(l int64) string {
			return strconv.FormatInt(l, 10)
		}
	}

	c.out.printStdout("Count: %v\n", count)
	c.out.printStdout("Total: %v\n", sizeToString(totalSize))

	if count == 0 {
		return nil
	}

	c.out.printStdout("Average: %v\n", sizeToString(totalSize/count))

	c.out.printStdout("Histogram:\n\n")

	var lastSize int64

	for _, size := range sizeThresholds {
		c.out.printStdout("%9v between %v and %v (total %v)\n",
			countMap[size]-countMap[lastSize],
			sizeToString(lastSize),
			sizeToString(size),
			sizeToString(totalSizeOfContentsUnder[size]-totalSizeOfContentsUnder[lastSize]),
		)

		lastSize = size
	}

	return nil
}
