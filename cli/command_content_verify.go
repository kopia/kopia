package cli

import (
	"context"
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
	progressInterval            time.Duration

	contentRange contentRangeFlags
}

func (c *commandContentVerify) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("verify", "Verify that each content is backed by a valid blob")

	cmd.Flag("parallel", "Parallelism").Default("16").IntVar(&c.contentVerifyParallel)
	cmd.Flag("full", "Full verification (including download)").BoolVar(&c.contentVerifyFull)
	cmd.Flag("include-deleted", "Include deleted contents").BoolVar(&c.contentVerifyIncludeDeleted)
	cmd.Flag("progress-interval", "Progress output interval").Default("3s").DurationVar(&c.progressInterval)
	c.contentRange.setup(cmd)
	cmd.Action(svc.directRepositoryReadAction(c.run))
}

func readBlobMap(ctx context.Context, br blob.Reader) (map[blob.ID]blob.Metadata, error) {
	blobMap := map[blob.ID]blob.Metadata{}

	log(ctx).Infof("Listing blobs...")

	if err := br.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		blobMap[bm.BlobID] = bm
		if len(blobMap)%10000 == 0 {
			log(ctx).Infof("  %v blobs...", len(blobMap))
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "unable to list blobs")
	}

	log(ctx).Infof("Listed %v blobs.", len(blobMap))

	return blobMap, nil
}

func (c *commandContentVerify) run(ctx context.Context, rep repo.DirectRepository) error {
	blobMap := map[blob.ID]blob.Metadata{}

	if !c.contentVerifyFull {
		m, err := readBlobMap(ctx, rep.BlobReader())
		if err != nil {
			return err
		}

		blobMap = m
	}

	verifiedCount := new(int32)
	successCount := new(int32)
	errorCount := new(int32)
	totalCount := new(int32)

	// start a goroutine that will estimate the number of contents we will be verifying.
	go func() {
		var tc int32

		if err := rep.ContentReader().IterateContents(ctx, content.IterateOptions{
			Range:          c.contentRange.contentIDRange(),
			IncludeDeleted: c.contentVerifyIncludeDeleted,
		}, func(ci content.Info) error {
			tc++
			return nil
		}); err != nil {
			log(ctx).Debugf("error estimating content count: %v", err)
			return
		}

		atomic.StoreInt32(totalCount, tc)
	}()

	log(ctx).Infof("Verifying all contents...")

	throttle := new(timetrack.Throttle)
	est := timetrack.Start()

	err := rep.ContentReader().IterateContents(ctx, content.IterateOptions{
		Range:          c.contentRange.contentIDRange(),
		Parallel:       c.contentVerifyParallel,
		IncludeDeleted: c.contentVerifyIncludeDeleted,
	}, func(ci content.Info) error {
		if err := c.contentVerify(ctx, rep.ContentReader(), ci, blobMap); err != nil {
			log(ctx).Errorf("error %v", err)
			atomic.AddInt32(errorCount, 1)
		} else {
			atomic.AddInt32(successCount, 1)
		}

		atomic.AddInt32(verifiedCount, 1)

		if throttle.ShouldOutput(c.progressInterval) {
			timings, ok := est.Estimate(float64(atomic.LoadInt32(verifiedCount)), float64(atomic.LoadInt32(totalCount)))
			if ok {
				log(ctx).Infof("  Verified %v of %v contents (%.1f%%), %v errors, remaining %v, ETA %v",
					atomic.LoadInt32(verifiedCount),
					atomic.LoadInt32(totalCount),
					timings.PercentComplete,
					atomic.LoadInt32(errorCount),
					timings.Remaining,
					formatTimestamp(timings.EstimatedEndTime),
				)
			} else {
				log(ctx).Infof("  Verified %v contents, %v errors, estimating...", atomic.LoadInt32(verifiedCount), atomic.LoadInt32(errorCount))
			}
		}

		return nil
	})
	if err != nil {
		return errors.Wrap(err, "iterate contents")
	}

	log(ctx).Infof("Finished verifying %v contents, found %v errors.", atomic.LoadInt32(verifiedCount), atomic.LoadInt32(errorCount))

	ec := atomic.LoadInt32(errorCount)
	if ec == 0 {
		return nil
	}

	return errors.Errorf("encountered %v errors", ec)
}

func (c *commandContentVerify) contentVerify(ctx context.Context, r content.Reader, ci content.Info, blobMap map[blob.ID]blob.Metadata) error {
	if c.contentVerifyFull {
		if _, err := r.GetContent(ctx, ci.GetContentID()); err != nil {
			return errors.Wrapf(err, "content %v is invalid", ci.GetContentID())
		}

		return nil
	}

	bi, ok := blobMap[ci.GetPackBlobID()]
	if !ok {
		return errors.Errorf("content %v depends on missing blob %v", ci.GetContentID(), ci.GetPackBlobID())
	}

	if int64(ci.GetPackOffset()+ci.GetPackedLength()) > bi.Length {
		return errors.Errorf("content %v out of bounds of its pack blob %v", ci.GetContentID(), ci.GetPackBlobID())
	}

	return nil
}
