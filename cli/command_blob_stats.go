package cli

import (
	"context"
	"fmt"
	"strconv"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

var (
	blobStatsCommand = blobCommands.Command("stats", "Content statistics")
	blobStatsRaw     = blobStatsCommand.Flag("raw", "Raw numbers").Short('r').Bool()
	blobStatsPrefix  = blobStatsCommand.Flag("prefix", "Blob name prefix").String()
)

func runBlobStatsCommand(ctx context.Context, rep *repo.DirectRepository) error {
	var sizeThreshold int64 = 10

	countMap := map[int64]int{}
	totalSizeOfContentsUnder := map[int64]int64{}

	var sizeThresholds []int64

	for i := 0; i < 8; i++ {
		sizeThresholds = append(sizeThresholds, sizeThreshold)
		countMap[sizeThreshold] = 0
		sizeThreshold *= 10
	}

	var totalSize, count int64

	if err := rep.Blobs.ListBlobs(
		ctx,
		blob.ID(*blobStatsPrefix),
		func(b blob.Metadata) error {
			totalSize += b.Length
			count++
			if count%10000 == 0 {
				printStderr("Got %v blobs...\n", count)
			}
			for s := range countMap {
				if b.Length < s {
					countMap[s]++
					totalSizeOfContentsUnder[s] += b.Length
				}
			}
			return nil
		}); err != nil {
		return err
	}

	sizeToString := units.BytesStringBase10
	if *blobStatsRaw {
		sizeToString = func(l int64) string { return strconv.FormatInt(l, 10) }
	}

	fmt.Println("Count:", count)
	fmt.Println("Total:", sizeToString(totalSize))

	if count == 0 {
		return nil
	}

	fmt.Println("Average:", sizeToString(totalSize/count))

	fmt.Printf("Histogram:\n\n")

	var lastSize int64

	for _, size := range sizeThresholds {
		fmt.Printf("%9v between %v and %v (total %v)\n",
			countMap[size]-countMap[lastSize],
			sizeToString(lastSize),
			sizeToString(size),
			sizeToString(totalSizeOfContentsUnder[size]-totalSizeOfContentsUnder[lastSize]),
		)

		lastSize = size
	}

	return nil
}

func init() {
	blobStatsCommand.Action(directRepositoryAction(runBlobStatsCommand))
}
