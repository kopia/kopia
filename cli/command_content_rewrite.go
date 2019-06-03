package cli

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

var (
	contentRewriteCommand     = contentCommands.Command("rewrite", "Rewrite content using most recent format")
	contentRewriteIDs         = contentRewriteCommand.Arg("contentID", "Identifiers of contents to rewrite").Strings()
	contentRewriteParallelism = contentRewriteCommand.Flag("parallelism", "Number of parallel workers").Default("16").Int()

	contentRewriteShortPacks    = contentRewriteCommand.Flag("short", "Rewrite contents from short packs").Bool()
	contentRewriteFormatVersion = contentRewriteCommand.Flag("format-version", "Rewrite contents using the provided format version").Default("-1").Int()
	contentRewritePackPrefix    = contentRewriteCommand.Flag("pack-prefix", "Only rewrite contents from pack blobs with a given prefix").String()
	contentRewriteDryRun        = contentRewriteCommand.Flag("dry-run", "Do not actually rewrite, only print what would happen").Short('n').Bool()
)

type contentInfoOrError struct {
	content.Info
	err error
}

func runContentRewriteCommand(ctx context.Context, rep *repo.Repository) error {
	cnt := getContentToRewrite(ctx, rep)

	var (
		mu          sync.Mutex
		totalBytes  int64
		failedCount int
	)

	var wg sync.WaitGroup

	for i := 0; i < *contentRewriteParallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for c := range cnt {
				if c.err != nil {
					log.Errorf("got error: %v", c.err)
					mu.Lock()
					failedCount++
					mu.Unlock()
					return
				}

				var optDeleted string
				if c.Deleted {
					optDeleted = " (deleted)"
				}

				printStderr("Rewriting content %v (%v bytes) from pack %v%v\n", c.ID, c.Length, c.PackBlobID, optDeleted)
				mu.Lock()
				totalBytes += int64(c.Length)
				mu.Unlock()
				if *contentRewriteDryRun {
					continue
				}
				if err := rep.Content.RewriteContent(ctx, c.ID); err != nil {
					log.Warningf("unable to rewrite content %q: %v", c.ID, err)
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

	return errors.Errorf("failed to rewrite %v contents", failedCount)
}

func getContentToRewrite(ctx context.Context, rep *repo.Repository) <-chan contentInfoOrError {
	ch := make(chan contentInfoOrError)
	go func() {
		defer close(ch)

		// get content IDs listed on command line
		findContentInfos(ctx, rep, ch, toContentIDs(*contentRewriteIDs))

		// add all content IDs from short packs
		if *contentRewriteShortPacks {
			threshold := uint32(rep.Content.Format.MaxPackSize * 6 / 10)
			findContentInShortPacks(rep, ch, threshold)
		}

		// add all blocks with given format version
		if *contentRewriteFormatVersion != -1 {
			findContentWithFormatVersion(rep, ch, *contentRewriteFormatVersion)
		}
	}()

	return ch
}

func toContentIDs(s []string) []content.ID {
	var result []content.ID
	for _, cid := range s {
		result = append(result, content.ID(cid))
	}
	return result
}

func findContentInfos(ctx context.Context, rep *repo.Repository, ch chan contentInfoOrError, contentIDs []content.ID) {
	for _, contentID := range contentIDs {
		i, err := rep.Content.ContentInfo(ctx, contentID)
		if err != nil {
			ch <- contentInfoOrError{err: errors.Wrapf(err, "unable to get info for content %q", contentID)}
		} else {
			ch <- contentInfoOrError{Info: i}
		}
	}
}

func findContentWithFormatVersion(rep *repo.Repository, ch chan contentInfoOrError, version int) {
	infos, err := rep.Content.ListContentInfos("", true)
	if err != nil {
		ch <- contentInfoOrError{err: errors.Wrap(err, "unable to list index blobs")}
		return
	}

	for _, b := range infos {
		if int(b.FormatVersion) == version && strings.HasPrefix(string(b.PackBlobID), *contentRewritePackPrefix) {
			ch <- contentInfoOrError{Info: b}
		}
	}
}

func findContentInShortPacks(rep *repo.Repository, ch chan contentInfoOrError, threshold uint32) {
	log.Debugf("listing contents...")
	infos, err := rep.Content.ListContentInfos("", true)
	if err != nil {
		ch <- contentInfoOrError{err: errors.Wrap(err, "unable to list index blobs")}
		return
	}

	log.Debugf("finding content in short pack blobs...")
	shortPackBlocks := findShortPackBlobs(infos, threshold)
	log.Debugf("found %v short pack blobs", len(shortPackBlocks))

	if len(shortPackBlocks) <= 1 {
		fmt.Printf("Nothing to do, found %v short pack blobs\n", len(shortPackBlocks))
	} else {
		for _, b := range infos {
			if shortPackBlocks[b.PackBlobID] && strings.HasPrefix(string(b.PackBlobID), *contentRewritePackPrefix) {
				ch <- contentInfoOrError{Info: b}
			}
		}
	}
}

func findShortPackBlobs(infos []content.Info, threshold uint32) map[blob.ID]bool {
	packUsage := map[blob.ID]uint32{}

	for _, bi := range infos {
		packUsage[bi.PackBlobID] += bi.Length
	}

	shortPackBlocks := map[blob.ID]bool{}

	for blobID, usage := range packUsage {
		if usage < threshold {
			shortPackBlocks[blobID] = true
		}
	}

	return shortPackBlocks
}

func init() {
	contentRewriteCommand.Action(repositoryAction(runContentRewriteCommand))
}
