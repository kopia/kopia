package cli

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo"
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
	var (
		totalCount atomic.Int32

		wg sync.WaitGroup
	)

	subctx, cancel := context.WithCancel(ctx)

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

	rep.DisableIndexRefresh()

	var throttle timetrack.Throttle

	est := timetrack.Start()

	if c.contentVerifyFull {
		c.contentVerifyPercent = 100.0
	}

	opts := content.VerifyOptions{
		ContentIDRange:            c.contentRange.contentIDRange(),
		ContentReadPercentage:     c.contentVerifyPercent,
		IncludeDeletedContents:    c.contentVerifyIncludeDeleted,
		ContentIterateParallelism: c.contentVerifyParallel,
		ProgressCallbackInterval:  1,

		ProgressCallback: func(vps content.VerifyProgressStats) {
			if !throttle.ShouldOutput(c.progressInterval) {
				return
			}

			verifiedCount := vps.SuccessCount + vps.ErrorCount

			timings, ok := est.Estimate(float64(verifiedCount), float64(totalCount.Load()))
			if ok {
				log(ctx).Infof("  Verified %v of %v contents (%.1f%%), %v errors, remaining %v, ETA %v",
					verifiedCount,
					totalCount.Load(),
					timings.PercentComplete,
					vps.ErrorCount,
					timings.Remaining,
					formatTimestamp(timings.EstimatedEndTime),
				)
			} else {
				log(ctx).Infof("  Verified %v contents, %v errors, estimating...", verifiedCount, vps.ErrorCount)
			}
		},
	}

	if err := rep.ContentReader().VerifyContents(ctx, opts); err != nil {
		return errors.Wrap(err, "verify contents")
	}

	return nil
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
