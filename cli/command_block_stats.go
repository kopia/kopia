package cli

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/kopia/kopia/repo"

	"github.com/kopia/kopia/internal/units"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	blockStatsCommand = blockCommands.Command("stats", "Block statistics")
	blockStatsKind    = blockStatsCommand.Flag("kind", "Kinds of blocks").Default("logical").Enum("all", "logical", "physical", "packed", "nonpacked", "packs")
	blockStatsRaw     = blockStatsCommand.Flag("raw", "Raw numbers").Short('r').Bool()
)

func runBlockStatsAction(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	blocks := rep.Blocks.ListBlocks("", *blockStatsKind)
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Length < blocks[j].Length })

	var sizeThreshold int64 = 10
	countMap := map[int64]int{}
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
			}
		}
	}

	fmt.Printf("Block statistics (%v)\n", *blockStatsKind)
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
		fmt.Printf("  %v blocks with size <%-12v\n", countMap[size], sizeToString(size))
	}

	return nil
}

func percentileSize(p int, blocks []repo.BlockInfo) int64 {
	pos := p * len(blocks) / 100

	return blocks[pos].Length
}

func init() {
	blockStatsCommand.Action(runBlockStatsAction)
}
