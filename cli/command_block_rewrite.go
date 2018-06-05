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
	blockRewriteCommand       = blockCommands.Command("rewrite", "Rewrite blocks using most recent format")
	blockRewriteIDs           = blockRewriteCommand.Arg("blockID", "Identifiers of blocks to rewrite").Strings()
	blockRewriteShortPacks    = blockRewriteCommand.Flag("short", "Rewrite blocks from short packs").Bool()
	blockRewriteFormatVersion = blockRewriteCommand.Flag("format-version", "Rewrite blocks using the provided format version").Default("-1").Int()
	blockRewritePackPrefix    = blockRewriteCommand.Flag("pack-prefix", "Only rewrite pack blocks with a given prefix").String()
	blockRewriteDryRun        = blockRewriteCommand.Flag("dry-run", "Do not actually rewrite, only print what would happen").Short('n').Bool()
)

func runRewriteBlocksAction(ctx context.Context, rep *repo.Repository) error {
	blocks, err := getBlocksToRewrite(ctx, rep)
	if err != nil {
		return fmt.Errorf("unable to determine blocks to rewrite: %v", err)
	}

	for _, b := range blocks {
		var optDeleted string
		if b.Deleted {
			optDeleted = " (deleted)"
		}
		fmt.Fprintf(os.Stderr, "Rewriting block %v from pack %v%v\n", b.BlockID, b.PackBlockID, optDeleted)
		if *blockRewriteDryRun {
			continue
		}
		if err := rep.Blocks.RewriteBlock(ctx, b.BlockID); err != nil {
			return fmt.Errorf("unable to rewrite block %q: %v", b.BlockID, err)
		}
	}

	return nil
}

func getBlocksToRewrite(ctx context.Context, rep *repo.Repository) ([]block.Info, error) {
	var result []block.Info
	for _, blockID := range *blockRewriteIDs {
		i, err := rep.Blocks.BlockInfo(ctx, blockID)
		if err != nil {
			return nil, fmt.Errorf("unable to get info for block %q: %v", blockID, err)
		}
		result = append(result, i)
	}

	if *blockRewriteShortPacks {
		infos, err := rep.Blocks.ListBlockInfos("", true)
		if err != nil {
			return nil, fmt.Errorf("unable to list index blocks: %v", err)
		}

		threshold := uint32(rep.Blocks.Format.MaxPackSize * 8 / 10)
		shortPackBlocks, err := findShortPackBlocks(infos, threshold)
		if err != nil {
			return nil, fmt.Errorf("unable to find short pack blocks: %v", err)
		}
		log.Printf("found %v short pack blocks", len(shortPackBlocks))

		if len(shortPackBlocks) <= 1 {
			fmt.Printf("Nothing to do, found %v short pack blocks\n", len(shortPackBlocks))
		} else {
			for _, b := range infos {
				if shortPackBlocks[b.PackBlockID] && strings.HasPrefix(string(b.PackBlockID), *blockRewritePackPrefix) {
					result = append(result, b)
				}
			}
		}
	}

	if *blockRewriteFormatVersion != -1 {
		infos, err := rep.Blocks.ListBlockInfos("", true)
		if err != nil {
			return nil, fmt.Errorf("unable to list index blocks: %v", err)
		}

		for _, b := range infos {
			if int(b.FormatVersion) == *blockRewriteFormatVersion && strings.HasPrefix(string(b.PackBlockID), *blockRewritePackPrefix) {
				result = append(result, b)
			}
		}
	}

	return result, nil
}

func findShortPackBlocks(infos []block.Info, threshold uint32) (map[block.PhysicalBlockID]bool, error) {
	packUsage := map[block.PhysicalBlockID]uint32{}

	for _, bi := range infos {
		packUsage[bi.PackBlockID] += bi.Length
	}

	shortPackBlocks := map[block.PhysicalBlockID]bool{}

	for packBlockID, usage := range packUsage {
		if usage < threshold {
			shortPackBlocks[packBlockID] = true
		}
	}

	return shortPackBlocks, nil
}

func init() {
	blockRewriteCommand.Action(repositoryAction(runRewriteBlocksAction))
}
