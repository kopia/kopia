package cli

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/internal/units"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	blockStatsCommand = blockCommands.Command("stats", "Block statistics")
	blockStatsRaw     = blockStatsCommand.Flag("raw", "Raw numbers").Short('r').Bool()
	blockStatsGroup   = blockStatsCommand.Flag("group", "Display stats about blocks belonging to a given group").String()
)

func runBlockStatsAction(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	blocks, err := rep.Blocks.ListBlocks("")
	if err != nil {
		return err
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Length < blocks[j].Length })

	var sizeThreshold int64 = 10
	countMap := map[int64]int{}
	totalSizeOfBlocksUnder := map[int64]int64{}
	var sizeThresholds []int64
	for i := 0; i < 8; i++ {
		sizeThresholds = append(sizeThresholds, sizeThreshold)
		countMap[sizeThreshold] = 0
		sizeThreshold *= 10
	}

	var totalSize int64
	for _, b := range blocks {
		totalSize += b.Length
		for s := range countMap {
			if b.Length < s {
				countMap[s]++
				totalSizeOfBlocksUnder[s] += b.Length
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
		fmt.Printf("  %v blocks with size <%v (total %v)\n", countMap[size], sizeToString(size), sizeToString(totalSizeOfBlocksUnder[size]))
	}

	return nil
}

func percentileSize(p int, blocks []block.Info) int64 {
	pos := p * len(blocks) / 100

	return blocks[pos].Length
}

func init() {
	blockStatsCommand.Action(runBlockStatsAction)
}
