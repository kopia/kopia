package cli

import (
	"context"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
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

const shortPackThresholdPercent = 60 // blocks below 60% of max block size are considered to be 'short

type contentInfoOrError struct {
	content.Info
	err error
}

func runContentRewriteCommand(ctx context.Context, rep *repo.DirectRepository) error {
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
					log(ctx).Errorf("got error: %v", c.err)
					mu.Lock()
					failedCount++
					mu.Unlock()

					return
				}

				var optDeleted string
				if c.Deleted {
					optDeleted = " (deleted)"
				}

				printStderr("Rewriting content %v (%v bytes) from pack %v%v %v\n", c.ID, c.Length, c.PackBlobID, optDeleted, formatTimestamp(c.Timestamp()))
				mu.Lock()
				totalBytes += int64(c.Length)
				mu.Unlock()

				if *contentRewriteDryRun {
					continue
				}

				if err := rep.Content.RewriteContent(ctx, c.ID); err != nil {
					log(ctx).Warningf("unable to rewrite content %q: %v", c.ID, err)
					mu.Lock()
					failedCount++
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	printStderr("Total bytes rewritten %v\n", units.BytesStringBase10(totalBytes))

	if failedCount == 0 {
		return nil
	}

	return errors.Errorf("failed to rewrite %v contents", failedCount)
}

func getContentToRewrite(ctx context.Context, rep *repo.DirectRepository) <-chan contentInfoOrError {
	ch := make(chan contentInfoOrError)

	go func() {
		defer close(ch)

		// get content IDs listed on command line
		findContentInfos(ctx, rep, ch, toContentIDs(*contentRewriteIDs))

		// add all content IDs from short packs
		if *contentRewriteShortPacks {
			threshold := int64(rep.Content.Format.MaxPackSize * shortPackThresholdPercent / 100) //nolint:gomnd
			findContentInShortPacks(ctx, rep, ch, threshold)
		}

		// add all blocks with given format version
		if *contentRewriteFormatVersion != -1 {
			findContentWithFormatVersion(ctx, rep, ch, *contentRewriteFormatVersion)
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

func findContentInfos(ctx context.Context, rep *repo.DirectRepository, ch chan contentInfoOrError, contentIDs []content.ID) {
	for _, contentID := range contentIDs {
		i, err := rep.Content.ContentInfo(ctx, contentID)
		if err != nil {
			ch <- contentInfoOrError{err: errors.Wrapf(err, "unable to get info for content %q", contentID)}
		} else {
			ch <- contentInfoOrError{Info: i}
		}
	}
}

func findContentWithFormatVersion(ctx context.Context, rep *repo.DirectRepository, ch chan contentInfoOrError, version int) {
	_ = rep.Content.IterateContents(
		ctx,
		content.IterateOptions{IncludeDeleted: true},
		func(b content.Info) error {
			if int(b.FormatVersion) == version && strings.HasPrefix(string(b.PackBlobID), *contentRewritePackPrefix) {
				ch <- contentInfoOrError{Info: b}
			}
			return nil
		})
}

func findContentInShortPacks(ctx context.Context, rep *repo.DirectRepository, ch chan contentInfoOrError, threshold int64) {
	if err := rep.Content.IterateContentInShortPacks(ctx, threshold, func(ci content.Info) error {
		ch <- contentInfoOrError{Info: ci}
		return nil
	}); err != nil {
		ch <- contentInfoOrError{err: err}
		return
	}
}

func init() {
	contentRewriteCommand.Action(directRepositoryAction(runContentRewriteCommand))
}
