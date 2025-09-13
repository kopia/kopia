package content

import (
	"context"
	"math/rand"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

// VerifyOptions allows specifying the optional arguments for WriteManager.VerifyContent.
type VerifyOptions struct {
	ContentIDRange         IDRange // defaults to AllIDs when not specified
	ContentReadPercentage  float64
	IncludeDeletedContents bool

	ContentIterateParallelism int

	ProgressCallback func(VerifyProgressStats)
	// Number of contents that need to be processed between calls to ProgressCallback.
	// For example, with a ProgressCallbackInterval of 1000, ProgressCallback
	// is called once for every 1000 contents that are processed.
	ProgressCallbackInterval uint32
}

// VerifyProgressStats contains progress counters that are passed to the
// progress callback used in WriteManager.VerifyContent.
type VerifyProgressStats struct {
	ErrorCount   uint32
	SuccessCount uint32
}

// VerifyContents checks whether contents are backed by valid blobs.
func (bm *WriteManager) VerifyContents(ctx context.Context, o VerifyOptions) error {
	var v contentVerifier

	return v.verifyContents(ctx, bm, o)
}

var errMissingPacks = errors.New("the repository is corrupted, it is missing pack blobs with index-referenced content")

type contentVerifier struct {
	bm *WriteManager

	existingPacks map[blob.ID]blob.Metadata

	progressCallback         func(VerifyProgressStats)
	progressCallbackInterval uint32

	contentReadProbability float64

	// content verification stats
	successCount atomic.Uint32
	errorCount   atomic.Uint32

	verifiedCount atomic.Uint32 // used for calling the progress callback at the specified interval.

	log logging.Logger
}

func (v *contentVerifier) verifyContents(ctx context.Context, bm *WriteManager, o VerifyOptions) error {
	existingPacks, err := blob.ReadBlobMap(ctx, bm.st)
	if err != nil {
		return errors.Wrap(err, "unable to get blob metadata map")
	}

	v.log = logging.Module("content/verify")(ctx)
	v.bm = bm
	v.existingPacks = existingPacks
	v.progressCallback = o.ProgressCallback
	v.contentReadProbability = max(o.ContentReadPercentage/100, 0) //nolint:mnd

	if o.ProgressCallback != nil {
		v.progressCallbackInterval = o.ProgressCallbackInterval
	}

	v.log.Info("Verifying contents...")

	itOpts := IterateOptions{
		Range:          o.ContentIDRange,
		Parallel:       o.ContentIterateParallelism,
		IncludeDeleted: o.IncludeDeletedContents,
	}

	cb := func(ci Info) error {
		v.verify(ctx, ci)

		return nil
	}

	err = bm.IterateContents(ctx, itOpts, cb)

	ec := v.errorCount.Load()
	contentCount := v.successCount.Load() + ec

	v.log.Infof("Finished verifying %v contents, found %v errors.", contentCount, ec)

	if err != nil {
		return err
	}

	if ec != 0 {
		return errors.Wrapf(errMissingPacks, "encountered %v errors", ec)
	}

	return nil
}

// verifies a content, updates the corresponding counter stats and it may call
// the progress callback.
func (v *contentVerifier) verify(ctx context.Context, ci Info) {
	v.verifyContentImpl(ctx, ci)

	count := v.verifiedCount.Add(1)

	if v.progressCallbackInterval > 0 && count%v.progressCallbackInterval == 0 {
		s := VerifyProgressStats{
			SuccessCount: v.successCount.Load(),
			ErrorCount:   v.errorCount.Load(),
		}

		v.progressCallback(s)
	}
}

func (v *contentVerifier) verifyContentImpl(ctx context.Context, ci Info) {
	bi, found := v.existingPacks[ci.PackBlobID]
	if !found {
		v.errorCount.Add(1)
		v.log.Errorf("content %v depends on missing blob %v", ci.ContentID, ci.PackBlobID)

		return
	}

	if int64(ci.PackOffset+ci.PackedLength) > bi.Length {
		v.errorCount.Add(1)
		v.log.Errorf("content %v out of bounds of its pack blob %v", ci.ContentID, ci.PackBlobID)

		return
	}

	//nolint:gosec
	if v.contentReadProbability > 0 && rand.Float64() < v.contentReadProbability {
		if _, err := v.bm.GetContent(ctx, ci.ContentID); err != nil {
			v.errorCount.Add(1)
			v.log.Errorf("content %v is invalid: %v", ci.ContentID, err)

			return
		}
	}

	v.successCount.Add(1)
}
