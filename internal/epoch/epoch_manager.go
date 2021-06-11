// Package epoch manages repository epochs.
// It implements protocol described https://github.com/kopia/kopia/issues/1090 and is intentionally
// separate from 'content' package to be able to test in isolation.
package epoch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

// Parameters encapsulates all parameters that influence the behavior of epoch manager.
type Parameters struct {
	// how frequently each client will list blobs to determine the current epoch.
	EpochRefreshFrequency time.Duration

	// number of epochs between full checkpoints.
	FullCheckpointFrequency int

	// do not delete uncompacted blobs if the corresponding compacted blob age is less than this.
	CleanupSafetyMargin time.Duration

	// minimum duration of an epoch
	MinEpochDuration time.Duration

	// advance epoch if number of files exceeds this
	EpochAdvanceOnCountThreshold int

	// advance epoch if total size of files exceeds this.
	EpochAdvanceOnTotalSizeBytesThreshold int64

	// number of blobs to delete in parallel during cleanup
	DeleteParallelism int
}

// nolint:gomnd
var defaultParams = Parameters{
	EpochRefreshFrequency:                 20 * time.Minute,
	FullCheckpointFrequency:               7,
	CleanupSafetyMargin:                   1 * time.Hour,
	MinEpochDuration:                      6 * time.Hour,
	EpochAdvanceOnCountThreshold:          100,
	EpochAdvanceOnTotalSizeBytesThreshold: 10 << 20,
	DeleteParallelism:                     4,
}

// snapshot captures a point-in time snapshot of a repository indexes, including current epoch
// information and existing checkpoints.
type snapshot struct {
	WriteEpoch                int                     `json:"writeEpoch"`
	LatestFullCheckpointEpoch int                     `json:"latestCheckpointEpoch"`
	FullCheckpointSets        map[int][]blob.Metadata `json:"fullCheckpointSets"`
	SingleEpochCompactionSets map[int][]blob.Metadata `json:"singleEpochCompactionSets"`
	EpochStartTime            map[int]time.Time       `json:"epochStartTimes"`
	ValidUntil                time.Time               `json:"validUntil"` // time after which the contents of this struct are no longer valid
}

func (cs *snapshot) isSettledEpochNumber(epoch int) bool {
	return epoch <= cs.WriteEpoch-numUnsettledEpochs
}

// Manager manages repository epochs.
type Manager struct {
	st       blob.Storage
	compact  CompactionFunc
	log      logging.Logger
	timeFunc func() time.Time
	params   Parameters

	// wait group that waits for all compaction and cleanup goroutines.
	backgroundWork sync.WaitGroup

	// mutable under lock, data invalid until refresh succeeds at least once.
	mu             sync.Mutex
	lastKnownState snapshot

	// counters keeping track of the number of times operations were too slow and had to
	// be retried, for testability.
	committedStateRefreshTooSlow *int32
	getCompleteIndexSetTooSlow   *int32
}

const (
	epochMarkerIndexBlobPrefix      blob.ID = "xe"
	uncompactedIndexBlobPrefix      blob.ID = "xn"
	singleEpochCompactionBlobPrefix blob.ID = "xs"
	fullCheckpointIndexBlobPrefix   blob.ID = "xf"

	numUnsettledEpochs = 2
)

// CompactionFunc merges the given set of index blobs into a new index blob set with a given prefix
// and writes them out as a set following naming convention established in 'complete_set.go'.
type CompactionFunc func(ctx context.Context, blobIDs []blob.ID, outputPrefix blob.ID) error

// Flush waits for all in-process compaction work to complete.
func (e *Manager) Flush() {
	// ensure all background compactions complete.
	e.backgroundWork.Wait()
}

// Refresh refreshes information about current epoch.
func (e *Manager) Refresh(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.refreshLocked(ctx)
}

// Cleanup cleans up the old indexes for which there's a compacted replacement.
func (e *Manager) Cleanup(ctx context.Context) error {
	cs, err := e.committedState(ctx)
	if err != nil {
		return err
	}

	return e.cleanupInternal(ctx, cs)
}

func (e *Manager) cleanupInternal(ctx context.Context, cs snapshot) error {
	eg, ctx := errgroup.WithContext(ctx)

	// delete epoch markers for epoch < current-1
	eg.Go(func() error {
		var toDelete []blob.ID

		if err := e.st.ListBlobs(ctx, epochMarkerIndexBlobPrefix, func(bm blob.Metadata) error {
			if n, ok := epochNumberFromBlobID(bm.BlobID); ok {
				if n < cs.WriteEpoch-1 {
					toDelete = append(toDelete, bm.BlobID)
				}
			}

			return nil
		}); err != nil {
			return errors.Wrap(err, "error listing epoch markers")
		}

		return errors.Wrap(blob.DeleteMultiple(ctx, e.st, toDelete, e.params.DeleteParallelism), "error deleting index blob marker")
	})

	// delete uncompacted indexes for epochs that already have single-epoch compaction
	// that was written sufficiently long ago.
	eg.Go(func() error {
		blobs, err := blob.ListAllBlobs(ctx, e.st, uncompactedIndexBlobPrefix)
		if err != nil {
			return errors.Wrap(err, "error listing uncompacted blobs")
		}

		var toDelete []blob.ID

		for _, bm := range blobs {
			if cs.safeToDeleteUncompactedBlob(bm, e.params.CleanupSafetyMargin) {
				toDelete = append(toDelete, bm.BlobID)
			}
		}

		if err := blob.DeleteMultiple(ctx, e.st, toDelete, e.params.DeleteParallelism); err != nil {
			return errors.Wrap(err, "unable to delete uncompacted blobs")
		}

		return nil
	})

	// delete single-epoch compacted indexes epoch numbers for which full-world state compacted exist
	if cs.LatestFullCheckpointEpoch > 0 {
		eg.Go(func() error {
			blobs, err := blob.ListAllBlobs(ctx, e.st, singleEpochCompactionBlobPrefix)
			if err != nil {
				return errors.Wrap(err, "error refreshing epochs")
			}

			var toDelete []blob.ID

			for _, bm := range blobs {
				epoch, ok := epochNumberFromBlobID(bm.BlobID)
				if !ok {
					continue
				}

				if epoch < cs.LatestFullCheckpointEpoch {
					toDelete = append(toDelete, bm.BlobID)
				}
			}

			return errors.Wrap(blob.DeleteMultiple(ctx, e.st, toDelete, e.params.DeleteParallelism), "error deleting single-epoch compacted blobs")
		})
	}

	return errors.Wrap(eg.Wait(), "error cleaning up index blobs")
}

func (cs *snapshot) safeToDeleteUncompactedBlob(bm blob.Metadata, safetyMargin time.Duration) bool {
	epoch, ok := epochNumberFromBlobID(bm.BlobID)
	if !ok {
		return false
	}

	if epoch < cs.LatestFullCheckpointEpoch {
		return true
	}

	cset := cs.SingleEpochCompactionSets[epoch]
	if cset == nil {
		// single-epoch compaction set does not exist for this epoch, don't delete.
		return false
	}

	// compaction set was written sufficiently long ago to be reliably discovered by all
	// other clients - we can delete uncompacted blobs for this epoch.
	compactionSetWriteTime := blob.MaxTimestamp(cset)

	return compactionSetWriteTime.Add(safetyMargin).Before(cs.EpochStartTime[cs.WriteEpoch])
}

func (e *Manager) refreshLocked(ctx context.Context) error {
	return errors.Wrap(retry.WithExponentialBackoffNoValue(ctx, "epoch manager refresh", func() error {
		return e.refreshAttemptLocked(ctx)
	}, retry.Always), "error refreshing")
}

func (e *Manager) loadWriteEpoch(ctx context.Context, cs *snapshot) error {
	blobs, err := blob.ListAllBlobs(ctx, e.st, epochMarkerIndexBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error loading write epoch")
	}

	for epoch, bm := range groupByEpochNumber(blobs) {
		cs.EpochStartTime[epoch] = bm[0].Timestamp

		if epoch > cs.WriteEpoch {
			cs.WriteEpoch = epoch
		}
	}

	return nil
}

func (e *Manager) loadFullCheckpoints(ctx context.Context, cs *snapshot) error {
	blobs, err := blob.ListAllBlobs(ctx, e.st, fullCheckpointIndexBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error loading full checkpoints")
	}

	for epoch, bms := range groupByEpochNumber(blobs) {
		if comp := findCompleteSetOfBlobs(bms); comp != nil {
			cs.FullCheckpointSets[epoch] = comp

			if epoch > cs.LatestFullCheckpointEpoch {
				cs.LatestFullCheckpointEpoch = epoch
			}
		}
	}

	return nil
}

func (e *Manager) loadSingleEpochCompactions(ctx context.Context, cs *snapshot) error {
	blobs, err := blob.ListAllBlobs(ctx, e.st, singleEpochCompactionBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error loading single-epoch compactions")
	}

	for epoch, bms := range groupByEpochNumber(blobs) {
		if comp := findCompleteSetOfBlobs(bms); comp != nil {
			cs.SingleEpochCompactionSets[epoch] = comp
		}
	}

	return nil
}

func (e *Manager) maybeStartFullCheckpointAsync(ctx context.Context, cs snapshot) {
	if cs.WriteEpoch-cs.LatestFullCheckpointEpoch < e.params.FullCheckpointFrequency {
		return
	}

	e.backgroundWork.Add(1)

	go func() {
		defer e.backgroundWork.Done()

		if err := e.generateFullCheckpointFromCommittedState(ctx, cs, cs.WriteEpoch-numUnsettledEpochs); err != nil {
			e.log.Errorf("unable to generate full checkpoint: %v, performance will be affected", err)
		}
	}()
}

func (e *Manager) maybeStartCleanupAsync(ctx context.Context, cs snapshot) {
	e.backgroundWork.Add(1)

	go func() {
		defer e.backgroundWork.Done()

		if err := e.cleanupInternal(ctx, cs); err != nil {
			e.log.Errorf("error cleaning up index blobs: %v, performance may be affected", err)
		}
	}()
}

// refreshAttemptLocked attempts to load the committedState of
// the index and updates `lastKnownState` state atomically when complete.
func (e *Manager) refreshAttemptLocked(ctx context.Context) error {
	cs := snapshot{
		WriteEpoch:                0,
		EpochStartTime:            map[int]time.Time{},
		SingleEpochCompactionSets: map[int][]blob.Metadata{},
		LatestFullCheckpointEpoch: 0,
		FullCheckpointSets:        map[int][]blob.Metadata{},
		ValidUntil:                e.timeFunc().Add(e.params.EpochRefreshFrequency),
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return e.loadWriteEpoch(ctx, &cs)
	})
	eg.Go(func() error {
		return e.loadSingleEpochCompactions(ctx, &cs)
	})
	eg.Go(func() error {
		return e.loadFullCheckpoints(ctx, &cs)
	})

	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "error refreshing")
	}

	if e.timeFunc().After(cs.ValidUntil) {
		atomic.AddInt32(e.committedStateRefreshTooSlow, 1)

		return errors.Errorf("refreshing committed state took too long")
	}

	e.lastKnownState = cs

	e.maybeStartFullCheckpointAsync(ctx, cs)
	e.maybeStartCleanupAsync(ctx, cs)

	e.log.Debugf("current epoch %v started at %v", cs.WriteEpoch, cs.EpochStartTime[cs.WriteEpoch])

	return nil
}

func (e *Manager) committedState(ctx context.Context) (snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.timeFunc().After(e.lastKnownState.ValidUntil) {
		if err := e.refreshLocked(ctx); err != nil {
			return snapshot{}, err
		}
	}

	return e.lastKnownState, nil
}

// Current returns the current epoch number.
func (e *Manager) Current(ctx context.Context) (int, error) {
	cs, err := e.committedState(ctx)
	if err != nil {
		return 0, err
	}

	return cs.WriteEpoch, nil
}

// GetCompleteIndexSet returns the set of blobs forming a complete index set up to the provided epoch number.
func (e *Manager) GetCompleteIndexSet(ctx context.Context, maxEpoch int) ([]blob.ID, error) {
	for {
		cs, err := e.committedState(ctx)
		if err != nil {
			return nil, err
		}

		result, err := e.getCompleteIndexSetForCommittedState(ctx, cs, maxEpoch)
		if e.timeFunc().Before(cs.ValidUntil) {
			return result, err
		}

		// We need to retry if local process took too long (e.g. because the machine went
		// to sleep at the wrong moment) and committed state is no longer valid.
		//
		// One scenario where this matters is cleanup: if determining the set of indexes takes
		// too long, it's possible for a cleanup process to delete some of the uncompacted
		// indexes that are still treated as authoritative according to old committed state.
		//
		// Retrying will re-examine the state of the world and re-do the logic.
		e.log.Debugf("GetCompleteIndexSet took too long, retrying to ensure correctness")
		atomic.AddInt32(e.getCompleteIndexSetTooSlow, 1)
	}
}

func (e *Manager) getCompleteIndexSetForCommittedState(ctx context.Context, cs snapshot, maxEpoch int) ([]blob.ID, error) {
	var (
		startEpoch int

		resultMutex sync.Mutex
		result      []blob.ID
	)

	for i := maxEpoch; i >= 0; i-- {
		if blobs := cs.FullCheckpointSets[i]; blobs != nil {
			result = append(result, blob.IDsFromMetadata(blobs)...)
			startEpoch = i + 1

			e.log.Debugf("using full checkpoint at epoch %v", i)

			break
		}
	}

	eg, ctx := errgroup.WithContext(ctx)

	e.log.Debugf("adding incremental state for epochs %v..%v", startEpoch, maxEpoch)

	for i := startEpoch; i <= maxEpoch; i++ {
		i := i

		eg.Go(func() error {
			s, err := e.getIndexesFromEpochInternal(ctx, cs, i)
			if err != nil {
				return errors.Wrapf(err, "error getting indexes for epoch %v", i)
			}

			resultMutex.Lock()
			result = append(result, s...)
			resultMutex.Unlock()

			return nil
		})
	}

	return result, errors.Wrap(eg.Wait(), "error getting indexes")
}

// WroteIndex is invoked after writing an index blob. It will validate whether the index was written
// in the correct epoch.
func (e *Manager) WroteIndex(ctx context.Context, bm blob.Metadata) error {
	cs, err := e.committedState(ctx)
	if err != nil {
		return err
	}

	epoch, ok := epochNumberFromBlobID(bm.BlobID)
	if !ok {
		return errors.Errorf("invalid blob ID written")
	}

	if cs.isSettledEpochNumber(epoch) {
		return errors.Errorf("index write took to long")
	}

	e.invalidate()

	return nil
}

func (e *Manager) getIndexesFromEpochInternal(ctx context.Context, cs snapshot, epoch int) ([]blob.ID, error) {
	// check if the epoch is old enough to possibly have compacted blobs
	epochSettled := cs.isSettledEpochNumber(epoch)
	if epochSettled && cs.SingleEpochCompactionSets[epoch] != nil {
		return blob.IDsFromMetadata(cs.SingleEpochCompactionSets[epoch]), nil
	}

	// load uncompacted blobs for this epoch
	uncompactedBlobs, err := blob.ListAllBlobs(ctx, e.st, uncompactedEpochBlobPrefix(epoch))
	if err != nil {
		return nil, errors.Wrapf(err, "error listing uncompacted indexes for epoch %v", epoch)
	}

	// Ignore blobs written after the epoch has been settled.
	//
	// Epochs N is 'settled' after epoch N+2 has been started and that makes N subject to compaction,
	// because at this point all clients will agree that we're in epoch N+1 or N+2.
	//
	// In a pathological case it's possible for client to write a blob for a 'settled' epoch if they:
	//
	// 1. determine current epoch number (N).
	// 2. go to sleep for a very long time, enough for epoch >=N+2 to become current.
	// 3. write blob for the epoch number N
	uncompactedBlobs = blobsWrittenBefore(
		uncompactedBlobs,
		cs.EpochStartTime[epoch+numUnsettledEpochs],
	)

	if epochSettled {
		e.backgroundWork.Add(1)

		go func() {
			defer e.backgroundWork.Done()

			e.log.Debugf("starting single-epoch compaction of %v", epoch)

			if err := e.compact(ctx, blob.IDsFromMetadata(uncompactedBlobs), compactedEpochBlobPrefix(epoch)); err != nil {
				e.log.Errorf("unable to compact blobs for epoch %v: %v, performance will be affected", epoch, err)
			}
		}()
	}

	advance := shouldAdvance(uncompactedBlobs, e.params.MinEpochDuration, e.params.EpochAdvanceOnCountThreshold, e.params.EpochAdvanceOnTotalSizeBytesThreshold)
	if advance && epoch == cs.WriteEpoch {
		if err := e.advanceEpoch(ctx, cs.WriteEpoch+1); err != nil {
			e.log.Errorf("unable to advance epoch: %v, performance will be affected", err)
		}
	}

	// return uncompacted blobs to the caller while we're compacting them in background
	return blob.IDsFromMetadata(uncompactedBlobs), nil
}

func (e *Manager) advanceEpoch(ctx context.Context, newEpoch int) error {
	blobID := blob.ID(fmt.Sprintf("%v%v", string(epochMarkerIndexBlobPrefix), newEpoch))

	if err := e.st.PutBlob(ctx, blobID, gather.FromSlice([]byte("epoch-marker"))); err != nil {
		return errors.Wrap(err, "error writing epoch marker")
	}

	e.invalidate()

	return nil
}

func (e *Manager) invalidate() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lastKnownState = snapshot{}
}

func (e *Manager) generateFullCheckpointFromCommittedState(ctx context.Context, cs snapshot, epoch int) error {
	e.log.Debugf("generating full checkpoint until epoch %v", epoch)

	completeSet, err := e.getCompleteIndexSetForCommittedState(ctx, cs, epoch)
	if err != nil {
		return errors.Wrap(err, "unable to get full checkpoint")
	}

	if e.timeFunc().After(cs.ValidUntil) {
		return errors.Errorf("not generating full checkpoint - the committed state is no longer valid")
	}

	if err := e.compact(ctx, completeSet, fullCheckpointBlobPrefix(epoch)); err != nil {
		return errors.Wrap(err, "unable to compact blobs")
	}

	return nil
}

func uncompactedEpochBlobPrefix(epoch int) blob.ID {
	return blob.ID(fmt.Sprintf("%v%v_", uncompactedIndexBlobPrefix, epoch))
}

func compactedEpochBlobPrefix(epoch int) blob.ID {
	return blob.ID(fmt.Sprintf("%v%v_", singleEpochCompactionBlobPrefix, epoch))
}

func fullCheckpointBlobPrefix(epoch int) blob.ID {
	return blob.ID(fmt.Sprintf("%v%v_", fullCheckpointIndexBlobPrefix, epoch))
}

// NewManager creates new epoch manager.
func NewManager(st blob.Storage, params Parameters, compactor CompactionFunc, sharedBaseLogger logging.Logger) *Manager {
	return &Manager{
		st:                           st,
		log:                          logging.WithPrefix("[epoch-manager] ", sharedBaseLogger),
		compact:                      compactor,
		timeFunc:                     clock.Now,
		params:                       params,
		getCompleteIndexSetTooSlow:   new(int32),
		committedStateRefreshTooSlow: new(int32),
	}
}
