package cli

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/internal/blockmgrpb"
	"github.com/kopia/kopia/repo"
)

var (
	blockIndexShowCommand = blockIndexCommands.Command("show", "List block indexes").Alias("cat")
	blockIndexShowSort    = blockIndexShowCommand.Flag("sort", "Sort order").Default("offset").Enum("offset", "blockID", "size")
	blockIndexShowIDs     = blockIndexShowCommand.Arg("id", "IDs of index blocks to show").Required().Strings()
	blockIndexShowRaw     = blockIndexShowCommand.Flag("raw", "Show raw block data").Bool()
)

type blockIndexEntryInfo struct {
	blockID string
	offset  uint32
	size    uint32
	inline  bool
	deleted bool
}

func getIndexBlocksToShow(ctx context.Context, rep *repo.Repository) ([]block.PhysicalBlockID, error) {
	var blockIDs []block.PhysicalBlockID
	for _, id := range *blockIndexShowIDs {
		blockIDs = append(blockIDs, block.PhysicalBlockID(id))
	}

	if len(blockIDs) == 1 && blockIDs[0] == "active" {
		b, err := rep.Blocks.ActiveIndexBlocks(ctx)
		if err != nil {
			return nil, err
		}

		sort.Slice(b, func(i, j int) bool {
			return b[i].Timestamp.Before(b[j].Timestamp)
		})

		blockIDs = nil
		for _, bi := range b {
			blockIDs = append(blockIDs, bi.BlockID)
		}
	}

	return blockIDs, nil
}

func runShowBlockIndexesAction(ctx context.Context, rep *repo.Repository) error {
	blockIDs, err := getIndexBlocksToShow(ctx, rep)
	if err != nil {
		return err
	}

	for _, blockID := range blockIDs {
		data, err := rep.Blocks.GetIndexBlock(ctx, blockID)
		if err != nil {
			return fmt.Errorf("can't read block %q: %v", blockID, err)
		}

		if *blockIndexShowRaw {
			os.Stdout.Write(data) //nolint:errcheck
		} else {
			var d blockmgrpb.Indexes
			if err := proto.Unmarshal(data, &d); err != nil {
				return err
			}

			for _, ndx := range d.Indexes {
				printIndex(ndx)
			}
		}
	}

	return nil
}

func printIndex(ndx *blockmgrpb.Index) {
	fmt.Printf("pack:%v len:%v created:%v\n", ndx.PackBlockId, ndx.PackLength, time.Unix(0, int64(ndx.CreateTimeNanos)).Local())
	var lines []blockIndexEntryInfo

	for _, it := range ndx.Items {
		if it.Deleted {
			lines = append(lines, blockIndexEntryInfo{blockID: decodeIndexBlockID(it.BlockId), deleted: true})
			continue
		}
		if len(it.Payload) > 0 {
			lines = append(lines, blockIndexEntryInfo{blockID: decodeIndexBlockID(it.BlockId), size: uint32(len(it.Payload)), inline: true})
		} else {
			lines = append(lines, blockIndexEntryInfo{blockID: decodeIndexBlockID(it.BlockId), offset: uint32(it.OffsetSize >> 32), size: uint32(it.OffsetSize)})
		}
	}
	sortIndexBlocks(lines)
	for _, l := range lines {
		if l.inline {
			fmt.Printf("  added %-40v size:%v (inline)\n", l.blockID, l.size)
		} else if l.deleted {
			fmt.Printf("  deleted %-40v\n", l.blockID)
		} else {
			fmt.Printf("  added %-40v offset:%-10v size:%v\n", l.blockID, l.offset, l.size)
		}
	}
}

func decodeIndexBlockID(b []byte) string {
	if b[0] == 0 {
		return fmt.Sprintf("%x", b[1:])
	}

	return string(b[0:1]) + fmt.Sprintf("%x", b[1:])
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
	blockIndexShowCommand.Action(repositoryAction(runShowBlockIndexesAction))
}
