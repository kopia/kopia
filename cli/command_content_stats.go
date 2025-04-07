package cli

import (
	"context"
	"strconv"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/compression"
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

type contentStatsTotals struct {
	originalSize int64
	packedSize   int64
	count        int64
}

func (c *commandContentStats) run(ctx context.Context, rep repo.DirectRepository) error {
	var (
		sizeThreshold uint32 = 10
		sizeBuckets   []uint32
	)

	for range 8 {
		sizeBuckets = append(sizeBuckets, sizeThreshold)
		sizeThreshold *= 10
	}

	grandTotal, byCompressionTotal, countMap, totalSizeOfContentsUnder, err := c.calculateStats(ctx, rep, sizeBuckets)
	if err != nil {
		return errors.Wrap(err, "error calculating totals")
	}

	sizeToString := units.BytesString[int64]
	if c.raw {
		sizeToString = func(l int64) string {
			return strconv.FormatInt(l, 10)
		}
	}

	c.out.printStdout("Count: %v\n", grandTotal.count)
	c.out.printStdout("Total Bytes: %v\n", sizeToString(grandTotal.originalSize))

	if grandTotal.packedSize < grandTotal.originalSize {
		c.out.printStdout(
			"Total Packed: %v (compression %v)\n",
			sizeToString(grandTotal.packedSize),
			formatCompressionPercentage(grandTotal.originalSize, grandTotal.packedSize))
	}

	if len(byCompressionTotal) > 1 {
		c.out.printStdout("By Method:\n")

		if bct := byCompressionTotal[content.NoCompression]; bct != nil {
			c.out.printStdout("  %-22v count: %v size: %v\n", "(uncompressed)", bct.count, sizeToString(bct.originalSize))
		}

		for hdrID, bct := range byCompressionTotal {
			cname := compression.HeaderIDToName[hdrID]
			if cname == "" {
				continue
			}

			c.out.printStdout("  %-22v count: %v size: %v packed: %v compression: %v\n",
				cname, bct.count,
				sizeToString(bct.originalSize),
				sizeToString(bct.packedSize),
				formatCompressionPercentage(bct.originalSize, bct.packedSize))
		}
	}

	if grandTotal.count == 0 {
		return nil
	}

	c.out.printStdout("Average: %v\n", sizeToString(grandTotal.originalSize/grandTotal.count))
	c.out.printStdout("Histogram:\n\n")

	var lastSize uint32

	for _, size := range sizeBuckets {
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

func (c *commandContentStats) calculateStats(ctx context.Context, rep repo.DirectRepository, sizeBuckets []uint32) (
	grandTotal contentStatsTotals,
	byCompressionTotal map[compression.HeaderID]*contentStatsTotals,
	countMap map[uint32]int,
	totalSizeOfContentsUnder map[uint32]int64,
	err error,
) {
	byCompressionTotal = make(map[compression.HeaderID]*contentStatsTotals)
	totalSizeOfContentsUnder = make(map[uint32]int64)
	countMap = make(map[uint32]int)

	for _, s := range sizeBuckets {
		countMap[s] = 0
	}

	err = rep.ContentReader().IterateContents(
		ctx,
		content.IterateOptions{
			Range: c.contentRange.contentIDRange(),
		},
		func(b content.Info) error {
			grandTotal.packedSize += int64(b.PackedLength)
			grandTotal.originalSize += int64(b.OriginalLength)
			grandTotal.count++

			bct := byCompressionTotal[b.CompressionHeaderID]
			if bct == nil {
				bct = &contentStatsTotals{}
				byCompressionTotal[b.CompressionHeaderID] = bct
			}

			bct.packedSize += int64(b.PackedLength)
			bct.originalSize += int64(b.OriginalLength)
			bct.count++

			for s := range countMap {
				if b.PackedLength < s {
					countMap[s]++
					totalSizeOfContentsUnder[s] += int64(b.PackedLength)
				}
			}

			return nil
		})

	//nolint:wrapcheck
	return grandTotal, byCompressionTotal, countMap, totalSizeOfContentsUnder, err
}
