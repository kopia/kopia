package maintenance

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobparam"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/internal/contentparam"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

const parallelContentRewritesCPUMultiplier = 2

// RewriteContentsOptions provides options for RewriteContents.
type RewriteContentsOptions struct {
	Parallel       int
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

type RewriteContentsStats struct {
	RewrittenCount uint32 `json:"rewrittenCount"`
	RewrittenSize  int64  `json:"rewrittenSize"`
	PreservedCount uint32 `json:"preservedCount"`
	PreservedSize  int64  `json:"preservedSize"`
}

func (rs *RewriteContentsStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.UInt32Field("rewrittenCount", rs.RewrittenCount)
	jw.Int64Field("rewrittenSize", rs.RewrittenSize)
	jw.UInt32Field("preservedCount", rs.PreservedCount)
	jw.Int64Field("preservedSize", rs.PreservedSize)
}

func (rs *RewriteContentsStats) MaintenanceSummary() string {
	return fmt.Sprintf("Rewritten %v(%v) contents, preserved %v(%v) contents", rs.RewrittenCount, rs.RewrittenSize, rs.PreservedCount, rs.PreservedSize)
}

// RewriteContents rewrites contents according to provided criteria and creates new
// blobs and index entries to point at them.
//
//nolint:funlen
func RewriteContents(ctx context.Context, rep repo.DirectRepositoryWriter, opt *RewriteContentsOptions, safety SafetyParameters) (*RewriteContentsStats, error) {
	ctx = contentlog.WithParams(ctx,
		logparam.String("span:content-rewrite", contentlog.RandomSpanID()))

	log := rep.LogManager().NewLogger("maintenance-content-rewrite")

	if opt == nil {
		return nil, errors.New("missing options")
	}

	if opt.ShortPacks {
		contentlog.Log(ctx, log, "Rewriting contents from short packs...")
	} else {
		contentlog.Log(ctx, log, "Rewriting contents...")
	}

	cnt := getContentToRewrite(ctx, rep, opt)

	var (
		mu             sync.Mutex
		totalBytes     int64
		totalCount     uint32
		preservedBytes int64
		preservedCount uint32
		failedCount    int
	)

	if opt.Parallel == 0 {
		opt.Parallel = runtime.NumCPU() * parallelContentRewritesCPUMultiplier
	}

	var wg sync.WaitGroup

	for range opt.Parallel {
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

				age := rep.Time().Sub(c.Timestamp())
				if age < safety.RewriteMinAge {
					contentlog.Log5(ctx, log,
						"Not rewriting content",
						contentparam.ContentID("contentID", c.ContentID),
						logparam.UInt32("bytes", c.PackedLength),
						blobparam.BlobID("packBlobID", c.PackBlobID),
						logparam.Bool("deleted", c.Deleted),
						logparam.Duration("age", age))

					mu.Lock()
					preservedBytes += int64(c.PackedLength)
					preservedCount++
					mu.Unlock()

					continue
				}

				contentlog.Log5(ctx, log,
					"Rewriting content",
					contentparam.ContentID("contentID", c.ContentID),
					logparam.UInt32("bytes", c.PackedLength),
					blobparam.BlobID("packBlobID", c.PackBlobID),
					logparam.Bool("deleted", c.Deleted),
					logparam.Duration("age", age))

				mu.Lock()
				totalBytes += int64(c.PackedLength)
				totalCount++
				mu.Unlock()

				if opt.DryRun {
					continue
				}

				if err := rep.ContentManager().RewriteContent(ctx, c.ContentID); err != nil {
					// provide option to ignore failures when rewriting deleted contents during maintenance
					// this is for advanced use only
					if os.Getenv("KOPIA_IGNORE_MAINTENANCE_REWRITE_ERROR") != "" && c.Deleted {
						contentlog.Log2(ctx, log,
							"IGNORED: unable to rewrite deleted content",
							contentparam.ContentID("contentID", c.ContentID),
							logparam.Error("error", err))
					} else {
						contentlog.Log2(ctx, log,
							"unable to rewrite content",
							contentparam.ContentID("contentID", c.ContentID),
							logparam.Error("error", err))

						mu.Lock()
						failedCount++
						mu.Unlock()
					}
				}
			}
		}()
	}

	wg.Wait()

	result := &RewriteContentsStats{
		RewrittenCount: totalCount,
		RewrittenSize:  totalBytes,
		PreservedCount: preservedCount,
		PreservedSize:  preservedBytes,
	}

	contentlog.Log1(ctx, log, "Content rewrite statistics", result)

	if failedCount == 0 {
		//nolint:wrapcheck
		if err := rep.ContentManager().Flush(ctx); err != nil {
			return nil, err
		}

		return result, nil
	}

	return nil, errors.Errorf("failed to rewrite %v contents", failedCount)
}

func getContentToRewrite(ctx context.Context, rep repo.DirectRepository, opt *RewriteContentsOptions) <-chan contentInfoOrError {
	ch := make(chan contentInfoOrError)

	go func() {
		defer close(ch)

		// get content IDs listed on command line
		findContentInfos(ctx, rep, ch, opt.ContentIDs)

		// add all content IDs from short packs
		if opt.ShortPacks {
			mp, mperr := rep.ContentReader().ContentFormat().GetMutableParameters(ctx)
			if mperr == nil {
				threshold := int64(mp.MaxPackSize * shortPackThresholdPercent / 100) //nolint:mnd
				findContentInShortPacks(ctx, rep, ch, threshold, opt)
			}
		}

		// add all blocks with given format version
		if opt.FormatVersion != 0 {
			findContentWithFormatVersion(ctx, rep, ch, opt)
		}
	}()

	return ch
}

func findContentInfos(ctx context.Context, rep repo.DirectRepository, ch chan contentInfoOrError, contentIDs []content.ID) {
	for _, contentID := range contentIDs {
		i, err := rep.ContentInfo(ctx, contentID)
		if err != nil {
			ch <- contentInfoOrError{err: errors.Wrapf(err, "unable to get info for content %q", contentID)}
		} else {
			ch <- contentInfoOrError{Info: i}
		}
	}
}

func findContentWithFormatVersion(ctx context.Context, rep repo.DirectRepository, ch chan contentInfoOrError, opt *RewriteContentsOptions) {
	_ = rep.ContentReader().IterateContents(
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

func findContentInShortPacks(ctx context.Context, rep repo.DirectRepository, ch chan contentInfoOrError, threshold int64, opt *RewriteContentsOptions) {
	var prefixes []blob.ID

	if opt.PackPrefix != "" {
		prefixes = append(prefixes, opt.PackPrefix)
	}

	var (
		packCountByPrefix = map[blob.ID]int{}
		firstPackByPrefix = map[blob.ID]content.PackInfo{}
	)

	err := rep.ContentReader().IteratePacks(
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

			prefix := pi.PackID[0:1]

			packCountByPrefix[prefix]++

			if packCountByPrefix[prefix] == 1 {
				// do not immediately compact the first pack, in case it's the only pack.
				firstPackByPrefix[prefix] = pi
				return nil
			}

			//nolint:mnd
			if packCountByPrefix[prefix] == 2 {
				// when we encounter the 2nd pack, emit contents from the first one too.
				for _, ci := range firstPackByPrefix[prefix].ContentInfos {
					ch <- contentInfoOrError{Info: ci}
				}

				firstPackByPrefix[prefix] = content.PackInfo{}
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
