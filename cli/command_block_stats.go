package cli

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
)

var (
	blockStatsCommand = blockCommands.Command("stats", "Block statistics")
	blockStatsRaw     = blockStatsCommand.Flag("raw", "Raw numbers").Short('r').Bool()
)

func runBlockStatsAction(ctx context.Context, rep *repo.Repository) error {
	blocks, err := rep.Blocks.ListBlockInfos("", true)
	if err != nil {
		return err
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Length < blocks[j].Length })

	var sizeThreshold uint32 = 10
	countMap := map[uint32]int{}
	totalSizeOfBlocksUnder := map[uint32]int64{}
	var sizeThresholds []uint32
	for i := 0; i < 8; i++ {
		sizeThresholds = append(sizeThresholds, sizeThreshold)
		countMap[sizeThreshold] = 0
		sizeThreshold *= 10
	}

	var totalSize int64
	for _, b := range blocks {
		totalSize += int64(b.Length)
		for s := range countMap {
			if b.Length < s {
				countMap[s]++
				totalSizeOfBlocksUnder[s] += int64(b.Length)
			}
		}
	}

	fmt.Printf("Block statistics\n")
	if len(blocks) == 0 {
		return nil
	}

	sizeToString := units.BytesStringBase10
	if *blockStatsRaw {
		sizeToString = func(l int64) string { return strconv.FormatInt(l, 10) }
	}

	fmt.Println("Size:          ")
	fmt.Println("  Total              ", sizeToString(totalSize))
	fmt.Println("  Average            ", sizeToString(totalSize/int64(len(blocks))))
	fmt.Println("  1st percentile     ", sizeToString(percentileSize(1, blocks)))
	fmt.Println("  5th percentile     ", sizeToString(percentileSize(5, blocks)))
	fmt.Println("  10th percentile    ", sizeToString(percentileSize(10, blocks)))
	fmt.Println("  50th percentile    ", sizeToString(percentileSize(50, blocks)))
	fmt.Println("  90th percentile    ", sizeToString(percentileSize(90, blocks)))
	fmt.Println("  95th percentile    ", sizeToString(percentileSize(95, blocks)))
	fmt.Println("  99th percentile    ", sizeToString(percentileSize(99, blocks)))

	fmt.Println("Counts:")
	for _, size := range sizeThresholds {
		fmt.Printf("  %v blocks with size <%v (total %v)\n", countMap[size], sizeToString(int64(size)), sizeToString(totalSizeOfBlocksUnder[size]))
	}

	return nil
}

func percentileSize(p int, blocks []block.Info) int64 {
	pos := p * len(blocks) / 100

	return int64(blocks[pos].Length)
}

func init() {
	blockStatsCommand.Action(repositoryAction(runBlockStatsAction))
}
