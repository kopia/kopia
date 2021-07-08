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
	"github.com/kopia/kopia/internal/completeset"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

// LatestEpoch represents the current epoch number in GetCompleteIndexSet.
const LatestEpoch = -1

const (
	initiaRefreshAttemptSleep      = 100 * time.Millisecond
	maxRefreshAttemptSleep         = 15 * time.Second
	maxRefreshAttemptSleepExponent = 1.5
)

// Parameters encapsulates all parameters that influence the behavior of epoch manager.
type Parameters struct {
	// whether epoch manager is enabled, must be true.
	Enabled bool

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

// Validate validates epoch parameters.
// nolint:gomnd
func (p *Parameters) Validate() error {
	if !p.Enabled {
		return nil
	}

	if p.MinEpochDuration < 10*time.Minute {
		return errors.Errorf("minimum epoch duration too low: %v", p.MinEpochDuration)
	}

	if p.EpochRefreshFrequency*3 > p.MinEpochDuration {
		return errors.Errorf("epoch refresh frequency too high, must be 1/3 or minimal epoch duration or less")
	}

	if p.FullCheckpointFrequency <= 0 {
		return errors.Errorf("invalid epoch checkpoint frequency")
	}

	if p.CleanupSafetyMargin*3 < p.EpochRefreshFrequency {
		return errors.Errorf("invalid cleanup safety margin, must be at least 3x epoch refresh frequency")
	}

	if p.EpochAdvanceOnCountThreshold < 10 {
		return errors.Errorf("epoch advance on count too low")
	}

	if p.EpochAdvanceOnTotalSizeBytesThreshold < 1<<20 {
		return errors.Errorf("epoch advance on size too low")
	}

	return nil
}

// DefaultParameters contains default epoch manager parameters.
// nolint:gomnd
var DefaultParameters = Parameters{
	Enabled:                               true,
	EpochRefreshFrequency:                 20 * time.Minute,
	FullCheckpointFrequency:               7,
	CleanupSafetyMargin:                   1 * time.Hour,
	MinEpochDuration:                      6 * time.Hour,
	EpochAdvanceOnCountThreshold:          20,
	EpochAdvanceOnTotalSizeBytesThreshold: 10 << 20,
	DeleteParallelism:                     4,
}

// CurrentSnapshot captures a point-in time snapshot of a repository indexes, including current epoch
// information and compaction set.
type CurrentSnapshot struct {
	WriteEpoch                 int                     `json:"writeEpoch"`
	UncompactedEpochSets       map[int][]blob.Metadata `json:"unsettled"`
	LongestRangeCheckpointSets []*RangeMetadata        `json:"longestRangeCheckpointSets"`
	SingleEpochCompactionSets  map[int][]blob.Metadata `json:"singleEpochCompactionSets"`
	EpochStartTime             map[int]time.Time       `json:"epochStartTimes"`
	ValidUntil                 time.Time               `json:"validUntil"` // time after which the contents of this struct are no longer valid
}

func (cs *CurrentSnapshot) isSettledEpochNumber(epoch int) bool {
	return epoch <= cs.WriteEpoch-numUnsettledEpochs
}

// Manager manages repository epochs.
type Manager struct {
	Params Parameters

	st       blob.Storage
	compact  CompactionFunc
	log      logging.Logger
	timeFunc func() time.Time

	// wait group that waits for all compaction and cleanup goroutines.
	backgroundWork sync.WaitGroup

	// mutable under lock, data invalid until refresh succeeds at least once.
	mu             sync.Mutex
	lastKnownState CurrentSnapshot

	// counters keeping track of the number of times operations were too slow and had to
	// be retried, for testability.
	committedStateRefreshTooSlow *int32
	getCompleteIndexSetTooSlow   *int32
	writeIndexTooSlow            *int32
}

// Index blob prefixes.
const (
	EpochMarkerIndexBlobPrefix      blob.ID = "xe"
	UncompactedIndexBlobPrefix      blob.ID = "xn"
	SingleEpochCompactionBlobPrefix blob.ID = "xs"
	RangeCheckpointIndexBlobPrefix  blob.ID = "xr"
)

// FirstEpoch is the number of the first epoch in a repository.
const FirstEpoch = 0

const numUnsettledEpochs = 2

// CompactionFunc merges the given set of index blobs into a new index blob set with a given prefix
// and writes them out as a set following naming convention established in 'complete_set.go'.
type CompactionFunc func(ctx context.Context, blobIDs []blob.ID, outputPrefix blob.ID) error

// Flush waits for all in-process compaction work to complete.
func (e *Manager) Flush() {
	// ensure all background compactions complete.
	e.backgroundWork.Wait()
}

// Current retrieves current snapshot.
func (e *Manager) Current(ctx context.Context) (CurrentSnapshot, error) {
	return e.committedState(ctx)
}

// ForceAdvanceEpoch advances current epoch unconditionally.
func (e *Manager) ForceAdvanceEpoch(ctx context.Context) error {
	cs, err := e.committedState(ctx)
	if err != nil {
		return err
	}

	e.Invalidate()

	if err := e.advanceEpoch(ctx, cs); err != nil {
		return errors.Wrap(err, "error advancing epoch")
	}

	return nil
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

func (e *Manager) cleanupInternal(ctx context.Context, cs CurrentSnapshot) error {
	eg, ctx := errgroup.WithContext(ctx)

	// find max timestamp recently written to the repository to establish storage clock.
	// we will be deleting blobs whose timestamps are sufficiently old enough relative
	// to this max time. This assumes that storage clock moves forward somewhat reasonably.
	var maxTime time.Time

	for _, v := range cs.UncompactedEpochSets {
		for _, bm := range v {
			if bm.Timestamp.After(maxTime) {
				maxTime = bm.Timestamp
			}
		}
	}

	if maxTime.IsZero() {
		return nil
	}

	// only delete blobs if a suitable replacement exists and has been written sufficiently
	// long ago. we don't want to delete blobs that are created too recently, because other clients
	// may have not observed them yet.
	maxReplacementTime := maxTime.Add(-e.Params.CleanupSafetyMargin)

	// delete epoch markers for epoch < current-1
	eg.Go(func() error {
		var toDelete []blob.ID

		if err := e.st.ListBlobs(ctx, EpochMarkerIndexBlobPrefix, func(bm blob.Metadata) error {
			if n, ok := epochNumberFromBlobID(bm.BlobID); ok {
				if n < cs.WriteEpoch-1 {
					toDelete = append(toDelete, bm.BlobID)
				}
			}

			return nil
		}); err != nil {
			return errors.Wrap(err, "error listing epoch markers")
		}

		return errors.Wrap(blob.DeleteMultiple(ctx, e.st, toDelete, e.Params.DeleteParallelism), "error deleting index blob marker")
	})

	// delete uncompacted indexes for epochs that already have single-epoch compaction
	// that was written sufficiently long ago.
	eg.Go(func() error {
		blobs, err := blob.ListAllBlobs(ctx, e.st, UncompactedIndexBlobPrefix)
		if err != nil {
			return errors.Wrap(err, "error listing uncompacted blobs")
		}

		var toDelete []blob.ID

		for _, bm := range blobs {
			if epoch, ok := epochNumberFromBlobID(bm.BlobID); ok {
				if blobSetWrittenEarlyEnough(cs.SingleEpochCompactionSets[epoch], maxReplacementTime) {
					toDelete = append(toDelete, bm.BlobID)
				}
			}
		}

		if err := blob.DeleteMultiple(ctx, e.st, toDelete, e.Params.DeleteParallelism); err != nil {
			return errors.Wrap(err, "unable to delete uncompacted blobs")
		}

		return nil
	})

	return errors.Wrap(eg.Wait(), "error cleaning up index blobs")
}

func blobSetWrittenEarlyEnough(replacementSet []blob.Metadata, maxReplacementTime time.Time) bool {
	max := blob.MaxTimestamp(replacementSet)
	if max.IsZero() {
		return false
	}

	// compaction set was written sufficiently long ago to be reliably discovered by all
	// other clients - we can delete uncompacted blobs for this epoch.
	return blob.MaxTimestamp(replacementSet).Before(maxReplacementTime)
}

func (e *Manager) refreshLocked(ctx context.Context) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "refreshLocked")
	}

	nextDelayTime := initiaRefreshAttemptSleep

	if !e.Params.Enabled {
		return errors.Errorf("epoch manager not enabled")
	}

	for err := e.refreshAttemptLocked(ctx); err != nil; err = e.refreshAttemptLocked(ctx) {
		if ctx.Err() != nil {
			return errors.Wrap(ctx.Err(), "refreshAttemptLocked")
		}

		e.log.Debugf("refresh attempt failed: %v, sleeping %v before next retry", err, nextDelayTime)

		nextDelayTime = time.Duration(float64(nextDelayTime) * maxRefreshAttemptSleepExponent)

		if nextDelayTime > maxRefreshAttemptSleep {
			nextDelayTime = maxRefreshAttemptSleep
		}
	}

	return nil
}

func (e *Manager) loadWriteEpoch(ctx context.Context, cs *CurrentSnapshot) error {
	blobs, err := blob.ListAllBlobs(ctx, e.st, EpochMarkerIndexBlobPrefix)
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

func (e *Manager) loadRangeCheckpoints(ctx context.Context, cs *CurrentSnapshot) error {
	blobs, err := blob.ListAllBlobs(ctx, e.st, RangeCheckpointIndexBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error loading full checkpoints")
	}

	e.log.Debugf("ranges: %v", blobs)

	var rangeCheckpointSets []*RangeMetadata

	for epoch1, m := range groupByEpochRanges(blobs) {
		for epoch2, bms := range m {
			if comp := completeset.FindFirst(bms); comp != nil {
				erm := &RangeMetadata{
					MinEpoch: epoch1,
					MaxEpoch: epoch2,
					Blobs:    comp,
				}

				rangeCheckpointSets = append(rangeCheckpointSets, erm)
			}
		}
	}

	cs.LongestRangeCheckpointSets = findLongestRangeCheckpoint(rangeCheckpointSets)

	return nil
}

func (e *Manager) loadSingleEpochCompactions(ctx context.Context, cs *CurrentSnapshot) error {
	blobs, err := blob.ListAllBlobs(ctx, e.st, SingleEpochCompactionBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error loading single-epoch compactions")
	}

	for epoch, bms := range groupByEpochNumber(blobs) {
		if comp := completeset.FindFirst(bms); comp != nil {
			cs.SingleEpochCompactionSets[epoch] = comp
		}
	}

	return nil
}

func (e *Manager) maybeGenerateNextRangeCheckpointAsync(ctx context.Context, cs CurrentSnapshot) {
	latestSettled := cs.WriteEpoch - numUnsettledEpochs
	if latestSettled < 0 {
		return
	}

	firstNonRangeCompacted := 0
	if len(cs.LongestRangeCheckpointSets) > 0 {
		firstNonRangeCompacted = cs.LongestRangeCheckpointSets[len(cs.LongestRangeCheckpointSets)-1].MaxEpoch + 1
	}

	if latestSettled-firstNonRangeCompacted < e.Params.FullCheckpointFrequency {
		e.log.Debugf("not generating range checkpoint")

		return
	}

	e.log.Debugf("generating range checkpoint")

	e.backgroundWork.Add(1)

	go func() {
		defer e.backgroundWork.Done()

		if err := e.generateRangeCheckpointFromCommittedState(ctx, cs, firstNonRangeCompacted, latestSettled); err != nil {
			e.log.Errorf("unable to generate full checkpoint: %v, performance will be affected", err)
		}
	}()
}

func (e *Manager) maybeOptimizeRangeCheckpointsAsync(ctx context.Context, cs CurrentSnapshot) {
}

func (e *Manager) maybeStartCleanupAsync(ctx context.Context, cs CurrentSnapshot) {
	e.backgroundWork.Add(1)

	go func() {
		defer e.backgroundWork.Done()

		if err := e.cleanupInternal(ctx, cs); err != nil {
			e.log.Errorf("error cleaning up index blobs: %v, performance may be affected", err)
		}
	}()
}

func (e *Manager) loadUncompactedEpochs(ctx context.Context, min, max int) (map[int][]blob.Metadata, error) {
	var mu sync.Mutex

	result := map[int][]blob.Metadata{}

	eg, ctx := errgroup.WithContext(ctx)

	for n := min; n <= max; n++ {
		n := n
		if n < 0 {
			continue
		}

		eg.Go(func() error {
			bm, err := blob.ListAllBlobs(ctx, e.st, UncompactedEpochBlobPrefix(n))
			if err != nil {
				return errors.Wrapf(err, "error listing uncompacted epoch %v", n)
			}

			mu.Lock()
			defer mu.Unlock()

			result[n] = bm
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, errors.Wrap(err, "error listing uncompacted epochs")
	}

	return result, nil
}

// refreshAttemptLocked attempts to load the committedState of
// the index and updates `lastKnownState` state atomically when complete.
func (e *Manager) refreshAttemptLocked(ctx context.Context) error {
	cs := CurrentSnapshot{
		WriteEpoch:                0,
		EpochStartTime:            map[int]time.Time{},
		UncompactedEpochSets:      map[int][]blob.Metadata{},
		SingleEpochCompactionSets: map[int][]blob.Metadata{},
		ValidUntil:                e.timeFunc().Add(e.Params.EpochRefreshFrequency),
	}

	e.log.Infof("refreshAttemptLocked")

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return e.loadWriteEpoch(ctx, &cs)
	})
	eg.Go(func() error {
		return e.loadSingleEpochCompactions(ctx, &cs)
	})
	eg.Go(func() error {
		return e.loadRangeCheckpoints(ctx, &cs)
	})

	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "error refreshing")
	}

	ues, err := e.loadUncompactedEpochs(ctx, cs.WriteEpoch-1, cs.WriteEpoch+1)
	if err != nil {
		return errors.Wrap(err, "error loading uncompacted epochs")
	}

	for epoch := range ues {
		ues[epoch] = blobsWrittenBefore(ues[epoch], cs.EpochStartTime[epoch+numUnsettledEpochs])
	}

	cs.UncompactedEpochSets = ues

	e.log.Debugf("current epoch %v, uncompacted epoch sets %v %v %v",
		cs.WriteEpoch,
		len(ues[cs.WriteEpoch-1]),
		len(ues[cs.WriteEpoch]),
		len(ues[cs.WriteEpoch+1]))

	if shouldAdvance(cs.UncompactedEpochSets[cs.WriteEpoch], e.Params.MinEpochDuration, e.Params.EpochAdvanceOnCountThreshold, e.Params.EpochAdvanceOnTotalSizeBytesThreshold) {
		if err := e.advanceEpoch(ctx, cs); err != nil {
			return errors.Wrap(err, "error advancing epoch")
		}
	}

	if e.timeFunc().After(cs.ValidUntil) {
		atomic.AddInt32(e.committedStateRefreshTooSlow, 1)

		return errors.Errorf("refreshing committed state took too long")
	}

	e.lastKnownState = cs

	e.maybeGenerateNextRangeCheckpointAsync(ctx, cs)
	e.maybeStartCleanupAsync(ctx, cs)
	e.maybeOptimizeRangeCheckpointsAsync(ctx, cs)

	return nil
}

func (e *Manager) advanceEpoch(ctx context.Context, cs CurrentSnapshot) error {
	blobID := blob.ID(fmt.Sprintf("%v%v", string(EpochMarkerIndexBlobPrefix), cs.WriteEpoch+1))

	if err := e.st.PutBlob(ctx, blobID, gather.FromSlice([]byte("epoch-marker"))); err != nil {
		return errors.Wrap(err, "error writing epoch marker")
	}

	return nil
}

func (e *Manager) committedState(ctx context.Context) (CurrentSnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.timeFunc().After(e.lastKnownState.ValidUntil) {
		e.log.Debugf("refreshing committed state because it's no longer valid")

		if err := e.refreshLocked(ctx); err != nil {
			return CurrentSnapshot{}, err
		}
	}

	return e.lastKnownState, nil
}

// GetCompleteIndexSet returns the set of blobs forming a complete index set up to the provided epoch number.
func (e *Manager) GetCompleteIndexSet(ctx context.Context, maxEpoch int) ([]blob.Metadata, error) {
	for {
		cs, err := e.committedState(ctx)
		if err != nil {
			return nil, err
		}

		if maxEpoch == LatestEpoch {
			maxEpoch = cs.WriteEpoch + 1
		}

		result, err := e.getCompleteIndexSetForCommittedState(ctx, cs, 0, maxEpoch)
		if e.timeFunc().Before(cs.ValidUntil) {
			e.log.Debugf("Complete Index Set for [%v..%v]: %v", 0, maxEpoch, blob.IDsFromMetadata(result))
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

// WriteIndex writes new index blob by picking the appropriate prefix based on current epoch.
func (e *Manager) WriteIndex(ctx context.Context, dataShards map[blob.ID]blob.Bytes) ([]blob.Metadata, error) {
	for {
		cs, err := e.committedState(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "error getting committed state")
		}

		var results []blob.Metadata

		for unprefixedBlobID, data := range dataShards {
			blobID := UncompactedEpochBlobPrefix(cs.WriteEpoch) + unprefixedBlobID

			if err := e.st.PutBlob(ctx, blobID, data); err != nil {
				return nil, errors.Wrap(err, "error writing index blob")
			}

			bm, err := e.st.GetMetadata(ctx, blobID)
			if err != nil {
				return nil, errors.Wrap(err, "error getting index metadata")
			}

			results = append(results, bm)
		}

		if !e.timeFunc().Before(cs.ValidUntil) {
			e.log.Debugf("write was too slow, retrying")
			atomic.AddInt32(e.writeIndexTooSlow, 1)

			continue
		}

		e.Invalidate()

		return results, nil
	}
}

// Invalidate ensures that all cached index information is discarded.
func (e *Manager) Invalidate() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.lastKnownState = CurrentSnapshot{}
}

func (e *Manager) getCompleteIndexSetForCommittedState(ctx context.Context, cs CurrentSnapshot, minEpoch, maxEpoch int) ([]blob.Metadata, error) {
	var result []blob.Metadata

	startEpoch := minEpoch

	for _, c := range cs.LongestRangeCheckpointSets {
		if c.MaxEpoch > startEpoch {
			result = append(result, c.Blobs...)
			startEpoch = c.MaxEpoch + 1
		}
	}

	eg, ctx := errgroup.WithContext(ctx)

	e.log.Debugf("adding incremental state for epochs %v..%v on top of %v", startEpoch, maxEpoch, result)
	cnt := maxEpoch - startEpoch + 1

	tmp := make([][]blob.Metadata, cnt)

	for i := 0; i < cnt; i++ {
		i := i
		ep := i + startEpoch

		eg.Go(func() error {
			s, err := e.getIndexesFromEpochInternal(ctx, cs, ep)
			if err != nil {
				return errors.Wrapf(err, "error getting indexes for epoch %v", ep)
			}

			tmp[i] = s

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, errors.Wrap(err, "error getting indexes")
	}

	for _, v := range tmp {
		result = append(result, v...)
	}

	return result, nil
}

func (e *Manager) getIndexesFromEpochInternal(ctx context.Context, cs CurrentSnapshot, epoch int) ([]blob.Metadata, error) {
	// check if the epoch is old enough to possibly have compacted blobs
	epochSettled := cs.isSettledEpochNumber(epoch)
	if epochSettled && cs.SingleEpochCompactionSets[epoch] != nil {
		return cs.SingleEpochCompactionSets[epoch], nil
	}

	// load uncompacted blobs for this epoch
	uncompactedBlobs, err := blob.ListAllBlobs(ctx, e.st, UncompactedEpochBlobPrefix(epoch))
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

	// return uncompacted blobs to the caller while we're compacting them in background
	return uncompactedBlobs, nil
}

func (e *Manager) generateRangeCheckpointFromCommittedState(ctx context.Context, cs CurrentSnapshot, minEpoch, maxEpoch int) error {
	e.log.Debugf("generating range checkpoint for %v..%v", minEpoch, maxEpoch)

	completeSet, err := e.getCompleteIndexSetForCommittedState(ctx, cs, minEpoch, maxEpoch)
	if err != nil {
		return errors.Wrap(err, "unable to get full checkpoint")
	}

	if e.timeFunc().After(cs.ValidUntil) {
		return errors.Errorf("not generating full checkpoint - the committed state is no longer valid")
	}

	if err := e.compact(ctx, blob.IDsFromMetadata(completeSet), rangeCheckpointBlobPrefix(minEpoch, maxEpoch)); err != nil {
		return errors.Wrap(err, "unable to compact blobs")
	}

	return nil
}

// UncompactedEpochBlobPrefix returns the prefix for uncompacted blobs of a given epoch.
func UncompactedEpochBlobPrefix(epoch int) blob.ID {
	return blob.ID(fmt.Sprintf("%v%v_", UncompactedIndexBlobPrefix, epoch))
}

func compactedEpochBlobPrefix(epoch int) blob.ID {
	return blob.ID(fmt.Sprintf("%v%v_", SingleEpochCompactionBlobPrefix, epoch))
}

func rangeCheckpointBlobPrefix(epoch1, epoch2 int) blob.ID {
	return blob.ID(fmt.Sprintf("%v%v_%v_", RangeCheckpointIndexBlobPrefix, epoch1, epoch2))
}

// NewManager creates new epoch manager.
func NewManager(st blob.Storage, params Parameters, compactor CompactionFunc, sharedBaseLogger logging.Logger) *Manager {
	log := logging.WithPrefix("[epoch-manager] ", sharedBaseLogger)

	return &Manager{
		st:                           st,
		log:                          log,
		compact:                      compactor,
		timeFunc:                     clock.Now,
		Params:                       params,
		getCompleteIndexSetTooSlow:   new(int32),
		committedStateRefreshTooSlow: new(int32),
		writeIndexTooSlow:            new(int32),
	}
}
