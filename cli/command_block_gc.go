package cli

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/repo"
	"github.com/rs/zerolog/log"
)

var (
	blockGarbageCollectCommand       = blockCommands.Command("gc", "Garbage-collect unused storage blocks")
	blockGarbageCollectCommandDelete = blockGarbageCollectCommand.Flag("delete", "Whether to delete unused block").String()
)

func runBlockGarbageCollectAction(ctx context.Context, rep *repo.Repository) error {
	infos, err := rep.Blocks.ListBlockInfos("", false)
	if err != nil {
		return fmt.Errorf("unable to list index blocks: %v", err)
	}

	usedPackBlocks := findPackBlocksInUse(infos)
	ch := rep.Storage.ListBlocks(ctx, "")

	var unused []string
	var totalBytes int64
	allPackBlocks := 0
	for bi := range ch {
		if bi.Error != nil {
			return fmt.Errorf("error listing storage blocks: %v", bi.Error)
		}

		if strings.HasPrefix(bi.BlockID, "n") {
			continue
		}
		if strings.HasPrefix(bi.BlockID, "kopia") {
			continue
		}

		allPackBlocks++

		u := usedPackBlocks[block.PhysicalBlockID(bi.BlockID)]
		if u > 0 {
			log.Printf("pack %v, in use by %v blocks", bi.BlockID, u)
			continue
		}

		totalBytes += int64(bi.Length)
		unused = append(unused, bi.BlockID)
	}
	fmt.Fprintf(os.Stderr, "Found %v/%v pack blocks in use.\n", len(usedPackBlocks), allPackBlocks)

	if len(unused) == 0 {
		fmt.Fprintf(os.Stderr, "No unused blocks found.\n")
		return nil
	}

	if *blockGarbageCollectCommandDelete != "yes" {
		for _, u := range unused {
			fmt.Fprintf(os.Stderr, "unused %v\n", u)
		}
		fmt.Fprintf(os.Stderr, "Would delete %v unused blocks (%v bytes), pass '--delete=yes' to actually delete.\n", len(unused), totalBytes)

		return nil
	}

	for _, u := range unused {
		fmt.Fprintf(os.Stderr, "Deleting unused block %q...\n", u)
		if err := rep.Storage.DeleteBlock(ctx, u); err != nil {
			return fmt.Errorf("unable to delete block %q: %v", u, err)
		}
	}

	return nil
}

func findPackBlocksInUse(infos []block.Info) map[block.PhysicalBlockID]int {
	packUsage := map[block.PhysicalBlockID]int{}

	for _, bi := range infos {
		packUsage[bi.PackBlockID]++
	}

	return packUsage
}

func init() {
	blockGarbageCollectCommand.Action(repositoryAction(runBlockGarbageCollectAction))
}
