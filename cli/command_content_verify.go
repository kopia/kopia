package cli

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

type commandContentVerify struct {
	contentVerifyParallel       int
	contentVerifyFull           bool
	contentVerifyIncludeDeleted bool
	contentVerifyPercent        float64
	progressInterval            time.Duration

	contentRange contentRangeFlags
}

func (c *commandContentVerify) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("verify", "Verify that each content is backed by a valid blob")

	cmd.Flag("parallel", "Parallelism").Default("16").IntVar(&c.contentVerifyParallel)
	cmd.Flag("full", "Full verification (including download)").BoolVar(&c.contentVerifyFull)
	cmd.Flag("include-deleted", "Include deleted contents").BoolVar(&c.contentVerifyIncludeDeleted)
	cmd.Flag("download-percent", "Download a percentage of files [0.0 .. 100.0]").Float64Var(&c.contentVerifyPercent)
	cmd.Flag("progress-interval", "Progress output interval").Default("3s").DurationVar(&c.progressInterval)
	c.contentRange.setup(cmd)
	cmd.Action(svc.directRepositoryReadAction(c.run))
}

func (c *commandContentVerify) run(ctx context.Context, rep repo.DirectRepository) error {
	blobMap := map[blob.ID]blob.Metadata{}
	downloadPercent := c.contentVerifyPercent

	if c.contentVerifyFull {
		downloadPercent = 100.0
	}

	blobMap, err := blob.ReadBlobMap(ctx, rep.BlobReader())
	if err != nil {
		return errors.Wrap(err, "unable to read blob map")
	}

	var (
		verifiedCount atomic.Int32
		successCount  atomic.Int32
		errorCount    atomic.Int32
		totalCount    atomic.Int32
	)

	subctx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup

	// ensure we cancel estimation goroutine and wait for it before returning
	defer func() {
		cancel()
		wg.Wait()
	}()

	// start a goroutine that will populate totalCount
	wg.Add(1)

	go func() {
		defer wg.Done()
		c.getTotalContentCount(subctx, rep, &totalCount)
	}()

	log(ctx).Info("Verifying all contents...")

	rep.DisableIndexRefresh()

	throttle := new(timetrack.Throttle)
	est := timetrack.Start()

	if err := rep.ContentReader().IterateContents(ctx, content.IterateOptions{
		Range:          c.contentRange.contentIDRange(),
		Parallel:       c.contentVerifyParallel,
		IncludeDeleted: c.contentVerifyIncludeDeleted,
	}, func(ci content.Info) error {
		if err := c.contentVerify(ctx, rep.ContentReader(), ci, blobMap, downloadPercent); err != nil {
			log(ctx).Errorf("error %v", err)
			errorCount.Add(1)
		} else {
			successCount.Add(1)
		}

		verifiedCount.Add(1)

		if throttle.ShouldOutput(c.progressInterval) {
			timings, ok := est.Estimate(float64(verifiedCount.Load()), float64(totalCount.Load()))
			if ok {
				log(ctx).Infof("  Verified %v of %v contents (%.1f%%), %v errors, remaining %v, ETA %v",
					verifiedCount.Load(),
					totalCount.Load(),
					timings.PercentComplete,
					errorCount.Load(),
					timings.Remaining,
					formatTimestamp(timings.EstimatedEndTime),
				)
			} else {
				log(ctx).Infof("  Verified %v contents, %v errors, estimating...", verifiedCount.Load(), errorCount.Load())
			}
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "iterate contents")
	}

	log(ctx).Infof("Finished verifying %v contents, found %v errors.", verifiedCount.Load(), errorCount.Load())

	ec := errorCount.Load()
	if ec == 0 {
		return nil
	}

	return errors.Errorf("encountered %v errors", ec)
}

func (c *commandContentVerify) getTotalContentCount(ctx context.Context, rep repo.DirectRepository, totalCount *atomic.Int32) {
	var tc int32

	if err := rep.ContentReader().IterateContents(ctx, content.IterateOptions{
		Range:          c.contentRange.contentIDRange(),
		IncludeDeleted: c.contentVerifyIncludeDeleted,
	}, func(_ content.Info) error {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "context error")
		}

		tc++
		return nil
	}); err != nil {
		log(ctx).Debugf("error estimating content count: %v", err)
		return
	}

	totalCount.Store(tc)
}

func (c *commandContentVerify) contentVerify(ctx context.Context, r content.Reader, ci content.Info, blobMap map[blob.ID]blob.Metadata, downloadPercent float64) error {
	bi, ok := blobMap[ci.PackBlobID]
	if !ok {
		return errors.Errorf("content %v depends on missing blob %v", ci.ContentID, ci.PackBlobID)
	}

	if int64(ci.PackOffset+ci.PackedLength) > bi.Length {
		return errors.Errorf("content %v out of bounds of its pack blob %v", ci.ContentID, ci.PackBlobID)
	}

	//nolint:gosec
	if 100*rand.Float64() < downloadPercent {
		if _, err := r.GetContent(ctx, ci.ContentID); err != nil {
			return errors.Wrapf(err, "content %v is invalid", ci.ContentID)
		}

		return nil
	}

	return nil
}
