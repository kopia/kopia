package maintenance

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

const defaultRewriteContentsMinAge = 2 * time.Hour

const parallelContentRewritesCPUMultiplier = 2

// RewriteContentsOptions provides options for RewriteContents
type RewriteContentsOptions struct {
	Parallel       int
	MinAge         time.Duration
	ContentIDs     []content.ID
	ContentIDRange content.IDRange
	PackPrefix     blob.ID
	ShortPacks     bool
	FormatVersion  int
	DryRun         bool
}

const shortPackThresholdPercent = 60 // blocks below 60% of max block size are considered to be 'short

type contentInfoOrError struct {
	content.Info
	err error
}

// RewriteContents rewrites contents according to provided criteria and creates new
// blobs and index entries to point at the
func RewriteContents(ctx context.Context, rep MaintainableRepository, opt *RewriteContentsOptions) error {
	if opt == nil {
		return errors.Errorf("missing options")
	}

	if opt.MinAge == 0 {
		opt.MinAge = defaultRewriteContentsMinAge
	}

	if opt.ShortPacks {
		log(ctx).Infof("Rewriting contents from short packs...")
	} else {
		log(ctx).Infof("Rewriting contents...")
	}

	cnt := getContentToRewrite(ctx, rep, opt)

	var (
		mu          sync.Mutex
		totalBytes  int64
		failedCount int
	)

	if opt.Parallel == 0 {
		opt.Parallel = runtime.NumCPU() * parallelContentRewritesCPUMultiplier
	}

	var wg sync.WaitGroup

	for i := 0; i < opt.Parallel; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for c := range cnt {
				if c.err != nil {
					mu.Lock()
					failedCount++
					mu.Unlock()

					return
				}

				var optDeleted string
				if c.Deleted {
					optDeleted = " (deleted)"
				}

				age := rep.Time().Sub(c.Timestamp())
				if age < opt.MinAge {
					log(ctx).Debugf("Not rewriting content %v (%v bytes) from pack %v%v %v, because it's too new.", c.ID, c.Length, c.PackBlobID, optDeleted, age)
					continue
				}

				log(ctx).Debugf("Rewriting content %v (%v bytes) from pack %v%v %v", c.ID, c.Length, c.PackBlobID, optDeleted, age)
				mu.Lock()
				totalBytes += int64(c.Length)
				mu.Unlock()

				if opt.DryRun {
					continue
				}

				if err := rep.ContentManager().RewriteContent(ctx, c.ID); err != nil {
					log(ctx).Infof("unable to rewrite content %q: %v", c.ID, err)
					mu.Lock()
					failedCount++
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	log(ctx).Debugf("Total bytes rewritten %v", units.BytesStringBase10(totalBytes))

	if failedCount == 0 {
		return rep.ContentManager().Flush(ctx)
	}

	return errors.Errorf("failed to rewrite %v contents", failedCount)
}

func getContentToRewrite(ctx context.Context, rep MaintainableRepository, opt *RewriteContentsOptions) <-chan contentInfoOrError {
	ch := make(chan contentInfoOrError)

	go func() {
		defer close(ch)

		// get content IDs listed on command line
		findContentInfos(ctx, rep, ch, opt.ContentIDs)

		// add all content IDs from short packs
		if opt.ShortPacks {
			threshold := int64(rep.ContentManager().Format.MaxPackSize * shortPackThresholdPercent / 100) //nolint:gomnd
			findContentInShortPacks(ctx, rep, ch, threshold, opt)
		}

		// add all blocks with given format version
		if opt.FormatVersion != 0 {
			findContentWithFormatVersion(ctx, rep, ch, opt)
		}
	}()

	return ch
}

func findContentInfos(ctx context.Context, rep MaintainableRepository, ch chan contentInfoOrError, contentIDs []content.ID) {
	for _, contentID := range contentIDs {
		i, err := rep.ContentManager().ContentInfo(ctx, contentID)
		if err != nil {
			ch <- contentInfoOrError{err: errors.Wrapf(err, "unable to get info for content %q", contentID)}
		} else {
			ch <- contentInfoOrError{Info: i}
		}
	}
}

func findContentWithFormatVersion(ctx context.Context, rep MaintainableRepository, ch chan contentInfoOrError, opt *RewriteContentsOptions) {
	_ = rep.ContentManager().IterateContents(
		ctx,
		content.IterateOptions{
			Range:          opt.ContentIDRange,
			IncludeDeleted: true,
		},
		func(b content.Info) error {
			if int(b.FormatVersion) == opt.FormatVersion && strings.HasPrefix(string(b.PackBlobID), string(opt.PackPrefix)) {
				ch <- contentInfoOrError{Info: b}
			}
			return nil
		})
}

func findContentInShortPacks(ctx context.Context, rep MaintainableRepository, ch chan contentInfoOrError, threshold int64, opt *RewriteContentsOptions) {
	var prefixes []blob.ID

	if opt.PackPrefix != "" {
		prefixes = append(prefixes, opt.PackPrefix)
	}

	err := rep.ContentManager().IteratePacks(
		ctx,
		content.IteratePackOptions{
			Prefixes:                           prefixes,
			IncludePacksWithOnlyDeletedContent: true,
			IncludeContentInfos:                true,
		},
		func(pi content.PackInfo) error {
			if pi.TotalSize >= threshold {
				return nil
			}

			for _, ci := range pi.ContentInfos {
				ch <- contentInfoOrError{Info: ci}
			}

			return nil
		},
	)

	if err != nil {
		ch <- contentInfoOrError{err: err}
		return
	}
}
