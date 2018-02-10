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

func runShowBlockIndexesAction(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	for _, blockID := range *blockIndexShowIDs {
		data, err := rep.Blocks.GetBlock(blockID)
		if err != nil {
			return fmt.Errorf("can't read block %q: %v", blockID, err)
		}

		var d blockmgrpb.Indexes
		if err := proto.Unmarshal(data, &d); err != nil {
			return err
		}

		for _, ndx := range d.Indexes {
			fmt.Printf("pack %v len: %v created %v\n", ndx.PackBlockId, ndx.PackLength, time.Unix(0, int64(ndx.CreateTimeNanos)).Local())
			type unpacked struct {
				blockID string
				offset  uint32
				size    uint32
			}
			var lines []unpacked

			for blk, os := range ndx.Items {
				lines = append(lines, unpacked{blk, uint32(os >> 32), uint32(os)})
			}
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
			for _, l := range lines {
				fmt.Printf("  added %-40v offset:%-10v size:%v\n", l.blockID, l.offset, l.size)
			}
			for _, del := range ndx.DeletedItems {
				fmt.Printf("  deleted %v\n", del)
			}
		}
	}

	return nil
}

func init() {
	blockIndexShowCommand.Action(runShowBlockIndexesAction)
}
