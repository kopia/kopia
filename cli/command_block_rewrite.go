package cli

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/block"
)

var (
	blockRewriteCommand     = blockCommands.Command("rewrite", "Rewrite blocks using most recent format")
	blockRewriteIDs         = blockRewriteCommand.Arg("blockID", "Identifiers of blocks to rewrite").Strings()
	blockRewriteParallelism = blockRewriteCommand.Flag("parallelism", "Number of parallel workers").Default("16").Int()

	blockRewriteShortPacks    = blockRewriteCommand.Flag("short", "Rewrite blocks from short packs").Bool()
	blockRewriteFormatVersion = blockRewriteCommand.Flag("format-version", "Rewrite blocks using the provided format version").Default("-1").Int()
	blockRewritePackPrefix    = blockRewriteCommand.Flag("pack-prefix", "Only rewrite pack blocks with a given prefix").String()
	blockRewriteDryRun        = blockRewriteCommand.Flag("dry-run", "Do not actually rewrite, only print what would happen").Short('n').Bool()
)

type blockInfoOrError struct {
	block.Info
	err error
}

func runRewriteBlocksAction(ctx context.Context, rep *repo.Repository) error {
	blocks := getBlocksToRewrite(ctx, rep)

	var (
		mu          sync.Mutex
		totalBytes  int64
		failedCount int
	)

	var wg sync.WaitGroup

	for i := 0; i < *blockRewriteParallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for b := range blocks {
				if b.err != nil {
					log.Errorf("got error: %v", b.err)
					mu.Lock()
					failedCount++
					mu.Unlock()
					return
				}

				var optDeleted string
				if b.Deleted {
					optDeleted = " (deleted)"
				}

				printStderr("Rewriting block %v (%v bytes) from pack %v%v\n", b.BlockID, b.Length, b.PackFile, optDeleted)
				mu.Lock()
				totalBytes += int64(b.Length)
				mu.Unlock()
				if *blockRewriteDryRun {
					continue
				}
				if err := rep.Blocks.RewriteBlock(ctx, b.BlockID); err != nil {
					log.Warningf("unable to rewrite block %q: %v", b.BlockID, err)
					mu.Lock()
					failedCount++
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	printStderr("Total bytes rewritten %v\n", totalBytes)

	if failedCount == 0 {
		return nil
	}

	return fmt.Errorf("failed to rewrite %v blocks", failedCount)
}

func getBlocksToRewrite(ctx context.Context, rep *repo.Repository) <-chan blockInfoOrError {
	ch := make(chan blockInfoOrError)
	go func() {
		defer close(ch)

		// get blocks listed on command line
		findBlockInfos(ctx, rep, ch, *blockRewriteIDs)

		// add all blocks from short packs
		if *blockRewriteShortPacks {
			threshold := uint32(rep.Blocks.Format.MaxPackSize * 6 / 10)
			findBlocksInShortPacks(ctx, rep, ch, threshold)
		}

		// add all blocks with given format version
		if *blockRewriteFormatVersion != -1 {
			findBlocksWithFormatVersion(ctx, rep, ch, *blockRewriteFormatVersion)
		}
	}()

	return ch
}

func findBlockInfos(ctx context.Context, rep *repo.Repository, ch chan blockInfoOrError, blockIDs []string) {
	for _, blockID := range blockIDs {
		i, err := rep.Blocks.BlockInfo(ctx, blockID)
		if err != nil {
			ch <- blockInfoOrError{err: fmt.Errorf("unable to get info for block %q: %v", blockID, err)}
		} else {
			ch <- blockInfoOrError{Info: i}
		}
	}
}

func findBlocksWithFormatVersion(ctx context.Context, rep *repo.Repository, ch chan blockInfoOrError, version int) {
	infos, err := rep.Blocks.ListBlockInfos("", true)
	if err != nil {
		ch <- blockInfoOrError{err: fmt.Errorf("unable to list index blocks: %v", err)}
		return
	}

	for _, b := range infos {
		if int(b.FormatVersion) == *blockRewriteFormatVersion && strings.HasPrefix(b.PackFile, *blockRewritePackPrefix) {
			ch <- blockInfoOrError{Info: b}
		}
	}
}

func findBlocksInShortPacks(ctx context.Context, rep *repo.Repository, ch chan blockInfoOrError, threshold uint32) {
	log.Debugf("listing blocks...")
	infos, err := rep.Blocks.ListBlockInfos("", true)
	if err != nil {
		ch <- blockInfoOrError{err: fmt.Errorf("unable to list index blocks: %v", err)}
		return
	}

	log.Debugf("finding blocks in short packs...")
	shortPackBlocks, err := findShortPackBlocks(infos, threshold)
	if err != nil {
		ch <- blockInfoOrError{err: fmt.Errorf("unable to find short pack blocks: %v", err)}
		return
	}
	log.Debugf("found %v short pack blocks", len(shortPackBlocks))

	if len(shortPackBlocks) <= 1 {
		fmt.Printf("Nothing to do, found %v short pack blocks\n", len(shortPackBlocks))
	} else {
		for _, b := range infos {
			if shortPackBlocks[b.PackFile] && strings.HasPrefix(b.PackFile, *blockRewritePackPrefix) {
				ch <- blockInfoOrError{Info: b}
			}
		}
	}
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
