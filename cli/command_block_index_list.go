package cli

import (
	"fmt"

	"github.com/kopia/kopia/block"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	blockIndexListCommand = blockIndexCommands.Command("list", "List block indexes").Alias("ls").Default()
	blockIndexListAll     = blockIndexListCommand.Flag("all", "List all blocks, not just active ones").Short('a').Bool()
	blockIndexListSummary = blockIndexListCommand.Flag("summary", "Display block summary").Bool()
)

func runListBlockIndexesAction(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	var blks []block.Info
	var err error

	if !*blockIndexListAll {
		blks, err = rep.Blocks.ActiveIndexBlocks()
	} else {
		blks, err = rep.Blocks.ListIndexBlocks()
	}

	if err != nil {
		return err
	}

	for _, b := range blks {
		fmt.Printf("%-70v %10v %v\n", b.BlockID, b.Length, b.Timestamp.Local().Format(timeFormatPrecise))
	}

	if *blockIndexListSummary {
		fmt.Printf("total %v blocks\n", len(blks))
	}

	return nil
}

func init() {
	blockIndexListCommand.Action(runListBlockIndexesAction)
}
