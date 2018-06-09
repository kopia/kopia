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

	var totalBytes int64
	failedCount := 0
	for _, b := range blocks {
		var optDeleted string
		if b.Deleted {
			optDeleted = " (deleted)"
		}
		fmt.Fprintf(os.Stderr, "Rewriting block %v (%v bytes) from pack %v%v\n", b.BlockID, b.Length, b.PackFile, optDeleted)
		totalBytes += int64(b.Length)
		if *blockRewriteDryRun {
			continue
		}
		if err := rep.Blocks.RewriteBlock(ctx, b.BlockID); err != nil {
			log.Warn().Msgf("unable to rewrite block %q: %v", b.BlockID, err)
			failedCount++
		}
	}

	fmt.Fprintf(os.Stderr, "Total bytes rewritten %v\n", totalBytes)

	if failedCount == 0 {
		return nil
	}

	return fmt.Errorf("failed to rewrite %v blocks", failedCount)
}

func getBlocksToRewrite(ctx context.Context, rep *repo.Repository) ([]block.Info, error) {
	// get blocks listed on command line
	result, err := getBlockInfos(ctx, rep, *blockRewriteIDs)
	if err != nil {
		return nil, err
	}

	// add all blocks from short packs
	if *blockRewriteShortPacks {
		threshold := uint32(rep.Blocks.Format.MaxPackSize * 6 / 10)
		info, err := getBlocksInShortPacks(ctx, rep, threshold)
		if err != nil {
			return nil, err
		}

		result = append(result, info...)
	}

	// add all blocks with given format version
	if *blockRewriteFormatVersion != -1 {
		info, err := getBlocksWithFormatVersion(ctx, rep, *blockRewriteFormatVersion)
		if err != nil {
			return nil, err
		}

		result = append(result, info...)
	}

	return result, nil
}

func getBlockInfos(ctx context.Context, rep *repo.Repository, blockIDs []string) ([]block.Info, error) {
	var result []block.Info
	for _, blockID := range blockIDs {
		i, err := rep.Blocks.BlockInfo(ctx, blockID)
		if err != nil {
			return nil, fmt.Errorf("unable to get info for block %q: %v", blockID, err)
		}
		result = append(result, i)
	}
	return result, nil
}

func getBlocksWithFormatVersion(ctx context.Context, rep *repo.Repository, version int) ([]block.Info, error) {
	var result []block.Info

	infos, err := rep.Blocks.ListBlockInfos("", true)
	if err != nil {
		return nil, fmt.Errorf("unable to list index blocks: %v", err)
	}

	for _, b := range infos {
		if int(b.FormatVersion) == *blockRewriteFormatVersion && strings.HasPrefix(b.PackFile, *blockRewritePackPrefix) {
			result = append(result, b)
		}
	}

	return result, nil
}

func getBlocksInShortPacks(ctx context.Context, rep *repo.Repository, threshold uint32) ([]block.Info, error) {
	var result []block.Info

	infos, err := rep.Blocks.ListBlockInfos("", true)
	if err != nil {
		return nil, fmt.Errorf("unable to list index blocks: %v", err)
	}

	shortPackBlocks, err := findShortPackBlocks(infos, threshold)
	if err != nil {
		return nil, fmt.Errorf("unable to find short pack blocks: %v", err)
	}
	log.Printf("found %v short pack blocks", len(shortPackBlocks))

	if len(shortPackBlocks) <= 1 {
		fmt.Printf("Nothing to do, found %v short pack blocks\n", len(shortPackBlocks))
	} else {
		for _, b := range infos {
			if shortPackBlocks[b.PackFile] && strings.HasPrefix(b.PackFile, *blockRewritePackPrefix) {
				result = append(result, b)
			}
		}
	}
	return result, nil
}

func findShortPackBlocks(infos []block.Info, threshold uint32) (map[string]bool, error) {
	packUsage := map[string]uint32{}

	for _, bi := range infos {
		packUsage[bi.PackFile] += bi.Length
	}

	shortPackBlocks := map[string]bool{}

	for packFile, usage := range packUsage {
		if usage < threshold {
			shortPackBlocks[packFile] = true
		}
	}

	return shortPackBlocks, nil
}

func init() {
	blockRewriteCommand.Action(repositoryAction(runRewriteBlocksAction))
}
