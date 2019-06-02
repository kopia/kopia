package cli

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

var (
	contentStatsCommand = contentCommands.Command("stats", "Content statistics")
	contentStatsRaw     = contentStatsCommand.Flag("raw", "Raw numbers").Short('r').Bool()
)

func runContentStatsCommand(ctx context.Context, rep *repo.Repository) error {
	contents, err := rep.Content.ListContentInfos("", true)
	if err != nil {
		return err
	}
	sort.Slice(contents, func(i, j int) bool {
		return contents[i].Length < contents[j].Length
	})

	var sizeThreshold uint32 = 10
	countMap := map[uint32]int{}
	totalSizeOfContentsUnder := map[uint32]int64{}
	var sizeThresholds []uint32
	for i := 0; i < 8; i++ {
		sizeThresholds = append(sizeThresholds, sizeThreshold)
		countMap[sizeThreshold] = 0
		sizeThreshold *= 10
	}

	var totalSize int64
	for _, b := range contents {
		totalSize += int64(b.Length)
		for s := range countMap {
			if b.Length < s {
				countMap[s]++
				totalSizeOfContentsUnder[s] += int64(b.Length)
			}
		}
	}

	fmt.Printf("Content statistics\n")
	if len(contents) == 0 {
		return nil
	}

	sizeToString := units.BytesStringBase10
	if *contentStatsRaw {
		sizeToString = func(l int64) string { return strconv.FormatInt(l, 10) }
	}

	fmt.Println("Size:          ")
	fmt.Println("  Total              ", sizeToString(totalSize))
	fmt.Println("  Average            ", sizeToString(totalSize/int64(len(contents))))
	fmt.Println("  1st percentile     ", sizeToString(percentileSize(1, contents)))
	fmt.Println("  5th percentile     ", sizeToString(percentileSize(5, contents)))
	fmt.Println("  10th percentile    ", sizeToString(percentileSize(10, contents)))
	fmt.Println("  50th percentile    ", sizeToString(percentileSize(50, contents)))
	fmt.Println("  90th percentile    ", sizeToString(percentileSize(90, contents)))
	fmt.Println("  95th percentile    ", sizeToString(percentileSize(95, contents)))
	fmt.Println("  99th percentile    ", sizeToString(percentileSize(99, contents)))

	fmt.Println("Counts:")
	for _, size := range sizeThresholds {
		fmt.Printf("  %v contents with size <%v (total %v)\n", countMap[size], sizeToString(int64(size)), sizeToString(totalSizeOfContentsUnder[size]))
	}

	return nil
}

func percentileSize(p int, contents []content.Info) int64 {
	pos := p * len(contents) / 100

	return int64(contents[pos].Length)
}

func init() {
	contentStatsCommand.Action(repositoryAction(runContentStatsCommand))
}
