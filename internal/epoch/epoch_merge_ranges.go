package epoch

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/maintenancestats"
)

// RangeCheckpointBlobPrefix returns the blob ID prefix of a range checkpoint covering epochs [epoch1, epoch2].
func RangeCheckpointBlobPrefix(epoch1, epoch2 int) blob.ID {
	return rangeCheckpointBlobPrefix(epoch1, epoch2)
}

// CleanupSupersededRangeCheckpoints deletes range checkpoint blobs that are not part of the longest chain
// and whose epochs are covered by a chain element written long enough ago to be seen by all clients.
func (e *Manager) CleanupSupersededRangeCheckpoints(ctx context.Context) (*maintenancestats.CleanupSupersededIndexesStats, error) {
	cs, err := e.committedState(ctx, 0)
	if err != nil {
		return nil, err
	}

	p, err := e.getParameters(ctx)
	if err != nil {
		return nil, err
	}

	// same storage clock and safety margin as CleanupSupersededIndexes
	maxTime := e.maxCleanupTime(cs)
	if maxTime.IsZero() {
		return nil, nil
	}

	maxReplacementTime := maxTime.Add(-p.CleanupSafetyMargin)

	blobs, err := blob.ListAllBlobs(ctx, e.st, RangeCheckpointIndexBlobPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "error listing range checkpoint blobs")
	}

	inChain := map[blob.ID]bool{}

	for _, r := range cs.LongestRangeCheckpointSets {
		for _, bm := range r.Blobs {
			inChain[bm.BlobID] = true
		}
	}

	var (
		toDelete         []blob.ID
		deletedTotalSize uint64
	)

	for epoch1, byMax := range groupByEpochRanges(blobs) {
		for epoch2, bms := range byMax {
			replacement := coveringRange(cs.LongestRangeCheckpointSets, epoch1, epoch2)
			if replacement == nil || !blobSetWrittenEarlyEnough(replacement.Blobs, maxReplacementTime) {
				continue
			}

			for _, bm := range bms {
				if !inChain[bm.BlobID] {
					toDelete = append(toDelete, bm.BlobID)
					deletedTotalSize += maintenancestats.ToUint64(bm.Length)
				}
			}
		}
	}

	contentlog.Log2(ctx, e.log, "Cleaning up superseded range checkpoints",
		logparam.Time("maxReplacementTime", maxReplacementTime),
		logparam.Int("blobCount", len(toDelete)))

	if err := blob.DeleteMultiple(ctx, e.st, toDelete, p.DeleteParallelism); err != nil {
		return nil, errors.Wrap(err, "unable to delete superseded range checkpoints")
	}

	return &maintenancestats.CleanupSupersededIndexesStats{
		MaxReplacementTime: maxReplacementTime,
		DeletedBlobCount:   uint64(len(toDelete)),
		DeletedTotalSize:   deletedTotalSize,
	}, nil
}

// coveringRange returns the chain element that covers epochs [epoch1, epoch2], if any.
func coveringRange(chain []*RangeMetadata, epoch1, epoch2 int) *RangeMetadata {
	for _, r := range chain {
		if r.MinEpoch <= epoch1 && epoch2 <= r.MaxEpoch {
			return r
		}
	}

	return nil
}
