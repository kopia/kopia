package cli

import (
	"fmt"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/kopia/kopia/internal/blockmgrpb"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	blockIndexShowCommand = blockIndexCommands.Command("show", "List block indexes").Alias("cat")
	blockIndexShowSort    = blockIndexShowCommand.Flag("sort", "Sort order").Default("offset").Enum("offset", "blockID", "size")
	blockIndexShowIDs     = blockIndexShowCommand.Arg("id", "IDs of index blocks to show").Required().Strings()
)

type blockIndexEntryInfo struct {
	blockID string
	offset  uint32
	size    uint32
	inline  bool
}

func runShowBlockIndexesAction(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close() //nolint: errcheck

	blockIDs := *blockIndexShowIDs
	if len(blockIDs) == 1 && blockIDs[0] == "active" {
		b, err := rep.Blocks.ActiveIndexBlocks()
		if err != nil {
			return err
		}

		sort.Slice(b, func(i, j int) bool {
			return b[i].Timestamp.Before(b[j].Timestamp)
		})

		blockIDs = nil
		for _, bi := range b {
			blockIDs = append(blockIDs, bi.BlockID)
		}
	}

	for _, blockID := range blockIDs {
		data, err := rep.Blocks.GetBlock(blockID)
		if err != nil {
			return fmt.Errorf("can't read block %q: %v", blockID, err)
		}

		var d blockmgrpb.Indexes
		if err := proto.Unmarshal(data, &d); err != nil {
			return err
		}

		for _, ndx := range d.Indexes {
			printIndex(ndx)
		}
	}

	return nil
}

func printIndex(ndx *blockmgrpb.Index) {
	fmt.Printf("pack:%v len:%v created:%v\n", ndx.PackBlockId, ndx.PackLength, time.Unix(0, int64(ndx.CreateTimeNanos)).Local())
	var lines []blockIndexEntryInfo

	for blk, os := range ndx.Items {
		lines = append(lines, blockIndexEntryInfo{blk, uint32(os >> 32), uint32(os), false})
	}
	for blk, d := range ndx.InlineItems {
		lines = append(lines, blockIndexEntryInfo{blk, 0, uint32(len(d)), true})
	}
	sortIndexBlocks(lines)
	for _, l := range lines {
		if l.inline {
			fmt.Printf("  added %-40v size:%v (inline)\n", l.blockID, l.size)
		} else {
			fmt.Printf("  added %-40v offset:%-10v size:%v\n", l.blockID, l.offset, l.size)
		}
	}
	for _, del := range ndx.DeletedItems {
		fmt.Printf("  deleted %v\n", del)
	}

}
func sortIndexBlocks(lines []blockIndexEntryInfo) {
	switch *blockIndexShowSort {
	case "offset":
		sort.Slice(lines, func(i, j int) bool {
			return lines[i].offset < lines[j].offset
		})
	case "blockID":
		sort.Slice(lines, func(i, j int) bool {
			return lines[i].blockID < lines[j].blockID
		})
	case "size":
		sort.Slice(lines, func(i, j int) bool {
			return lines[i].size < lines[j].size
		})
	}
}

func init() {
	blockIndexShowCommand.Action(runShowBlockIndexesAction)
}
