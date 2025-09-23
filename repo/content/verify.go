package content

import (
	"context"
	"math/rand"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/stats"
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
	successCount              atomic.Uint32
	readContentCount          atomic.Uint32
	missingPackContentCount   atomic.Uint32
	truncatedPackContentCount atomic.Uint32
	errorContentCount         atomic.Uint32

	// Per pack counts for content errors. Notice that a truncated pack, can
	// also appear in the corruptedPacks map. A missing pack can only be in
	// missingPacks, but not in the other sets.
	missingPacks   stats.CountersMap[blob.ID]
	truncatedPacks stats.CountersMap[blob.ID]
	corruptedPacks stats.CountersMap[blob.ID]

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

	contentInMissingPackCount := v.missingPackContentCount.Load()
	contentInTruncatedPackCount := v.truncatedPackContentCount.Load()
	contentErrorCount := v.errorContentCount.Load()
	totalErrorCount := contentInMissingPackCount + contentInTruncatedPackCount + contentErrorCount

	contentCount := v.verifiedCount.Load()

	v.log.Info("Finished verifying contents")
	v.log.Infow("verifyCounters:",
		"verifiedContents", contentCount,
		"totalErrorCount", totalErrorCount,
		"contentsInMissingPacks", contentInMissingPackCount,
		"contentsInTruncatedPacks", contentInTruncatedPackCount,
		"unreadableContents", contentErrorCount,
		"readContents", v.readContentCount.Load(),
		"missingPacks", v.missingPacks.Length(),
		"truncatedPacks", v.truncatedPacks.Length(),
		"corruptedPacks", v.corruptedPacks.Length(),
	)

	logCountMap(v.log, "missingPack", &v.missingPacks)
	logCountMap(v.log, "truncatedPack", &v.truncatedPacks)
	logCountMap(v.log, "corruptedPack", &v.corruptedPacks)

	if err != nil {
		return err
	}

	if totalErrorCount != 0 {
		return errors.Wrapf(errMissingPacks, "encountered %v errors", totalErrorCount)
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
			ErrorCount:   v.missingPackContentCount.Load() + v.truncatedPackContentCount.Load() + v.errorContentCount.Load(),
		}

		v.progressCallback(s)
	}
}

func (v *contentVerifier) verifyContentImpl(ctx context.Context, ci Info) {
	bi, found := v.existingPacks[ci.PackBlobID]
	if !found {
		v.missingPackContentCount.Add(1)
		v.missingPacks.Increment(ci.PackBlobID)
		v.log.Warnf("content %v depends on missing blob %v", ci.ContentID, ci.PackBlobID)

		return
	}

	if int64(ci.PackOffset+ci.PackedLength) > bi.Length {
		v.truncatedPackContentCount.Add(1)
		v.truncatedPacks.Increment(ci.PackBlobID)
		v.log.Warnf("content %v out of bounds of its pack blob %v", ci.ContentID, ci.PackBlobID)

		return
	}

	//nolint:gosec
	if v.contentReadProbability > 0 && rand.Float64() < v.contentReadProbability {
		v.readContentCount.Add(1)

		if _, err := v.bm.GetContent(ctx, ci.ContentID); err != nil {
			v.errorContentCount.Add(1)
			v.corruptedPacks.Increment(ci.PackBlobID)
			v.log.Warnf("content %v is invalid: %v", ci.ContentID, err)

			return
		}
	}

	v.successCount.Add(1)
}

// nothing is logged for an empty map.
func logCountMap(log logging.Logger, mapName string, m *stats.CountersMap[blob.ID]) {
	m.Range(func(packID blob.ID, contentCount uint32) bool {
		log.Warnw(mapName, "packID", packID, "numberOfReferencedContents", contentCount)

		return true
	})
}
