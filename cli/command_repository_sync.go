package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/stats"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/format"
)

type commandRepositorySyncTo struct {
	nextSyncOutputTime timetrack.Throttle

	repositorySyncUpdate               bool
	repositorySyncDelete               bool
	repositorySyncDryRun               bool
	repositorySyncParallelism          int
	repositorySyncDestinationMustExist bool
	repositorySyncTimes                bool
	repositorySyncRetentionMode        string
	repositorySyncRetentionPeriod      time.Duration
	repositorySyncExtendObjectLocks    bool

	lastSyncProgress  string
	syncProgressMutex sync.Mutex

	out      textOutput
	progress *cliProgress
}

func (c *commandRepositorySyncTo) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("sync-to", "Synchronizes the contents of this repository to another location")
	cmd.Flag("update", "Whether to update blobs present in destination and source if the source is newer.").Default("true").BoolVar(&c.repositorySyncUpdate)
	cmd.Flag("delete", "Whether to delete blobs present in destination but not source.").BoolVar(&c.repositorySyncDelete)
	cmd.Flag("dry-run", "Do not perform copying.").Short('n').BoolVar(&c.repositorySyncDryRun)
	cmd.Flag("parallel", "Copy parallelism.").Default("1").IntVar(&c.repositorySyncParallelism)
	cmd.Flag("must-exist", "Fail if destination does not have repository format blob.").BoolVar(&c.repositorySyncDestinationMustExist)
	cmd.Flag("times", "Synchronize blob times if supported.").BoolVar(&c.repositorySyncTimes)
	cmd.Flag("retention-mode", "Apply object-lock retention mode to synchronized blobs on the destination (requires object-lock capable storage such as S3).").EnumVar(&c.repositorySyncRetentionMode, blob.Governance.String(), blob.Compliance.String())
	cmd.Flag("retention-period", "Object-lock retention period for synchronized blobs (minimum 24h). Schedule syncs so locks are refreshed with a safety margin of at least 24h before expiry.").DurationVar(&c.repositorySyncRetentionPeriod)
	cmd.Flag("extend-object-locks", "Extend object locks on destination blobs already in sync (one retention-update API request per locked blob per run).").Default("true").BoolVar(&c.repositorySyncExtendObjectLocks)

	c.out.setup(svc)
	c.progress = svc.getProgress()

	for _, prov := range svc.storageProviders() {
		// Set up 'sync-to' subcommand
		f := prov.NewFlags()
		cc := cmd.Command(prov.Name, "Synchronize repository data to another repository in "+prov.Description)
		f.Setup(svc, cc)
		cc.Action(func(kpc *kingpin.ParseContext) error {
			return svc.runAppWithContext(kpc.SelectedCommand, func(ctx context.Context) error {
				st, err := f.Connect(ctx, false, 0)
				if err != nil {
					return errors.Wrap(err, "can't connect to storage")
				}

				rep, err := svc.openRepository(ctx, true)
				if err != nil {
					return errors.Wrap(err, "open repository")
				}

				defer rep.Close(ctx) //nolint:errcheck

				dr, ok := rep.(repo.DirectRepository)
				if !ok {
					return errors.New("sync only supports directly-connected repositories")
				}

				return c.runSyncWithStorage(ctx, dr.BlobReader(), st)
			})
		})
	}
}

const syncProgressInterval = 300 * time.Millisecond

func (c *commandRepositorySyncTo) runSyncWithStorage(ctx context.Context, src blob.Reader, dst blob.Storage) error {
	log(ctx).Info("Synchronizing repositories:")
	log(ctx).Infof("  Source:      %v", src.DisplayName())
	log(ctx).Infof("  Destination: %v", dst.DisplayName())

	if !c.repositorySyncDelete {
		log(ctx).Info("NOTE: By default no BLOBs are deleted, pass --delete to allow it.")
	}

	dst, blobcfg, err := c.setupDestinationRetention(ctx, dst)
	if err != nil {
		return err
	}

	if err := c.ensureRepositoriesHaveSameFormatBlob(ctx, src, dst); err != nil {
		return wrapObjectLockError(err)
	}

	log(ctx).Info("Looking for BLOBs to synchronize...")

	extendLocks := c.extendLocksEnabled(blobcfg)

	plan, err := c.computeSyncPlan(ctx, src, dst, extendLocks)
	if err != nil {
		return err
	}

	log(ctx).Infof(
		"  Found %v BLOBs to delete (%v), %v in sync (%v)",
		len(plan.blobsToDelete), units.BytesString(plan.totalDeleteBytes),
		plan.inSyncBlobs, units.BytesString(plan.inSyncBytes),
	)

	if extendLocks {
		log(ctx).Infof("  Found %v BLOBs to extend object locks on", len(plan.blobsToExtend))
	}

	if c.repositorySyncDryRun {
		return nil
	}

	log(ctx).Info("Copying...")

	c.beginSyncProgress()

	finalErr := c.runSyncBlobs(ctx, src, dst, plan.blobsToCopy, plan.blobsToDelete, plan.totalCopyBytes)

	c.finishSyncProcess()

	if finalErr == nil {
		finalErr = c.extendDestinationLocks(ctx, dst, plan.blobsToExtend, blobcfg)
	}

	return wrapObjectLockError(finalErr)
}

// syncPlan describes the work to be performed by a sync-to run.
type syncPlan struct {
	blobsToCopy    []blob.Metadata
	totalCopyBytes int64

	blobsToDelete    []blob.Metadata
	totalDeleteBytes int64

	blobsToExtend []blob.Metadata

	inSyncBlobs int
	inSyncBytes int64
}

// computeSyncPlan lists the source and destination blobs and classifies them
// into blobs to copy, delete and extend object locks on.
func (c *commandRepositorySyncTo) computeSyncPlan(ctx context.Context, src blob.Reader, dst blob.Storage, extendLocks bool) (*syncPlan, error) {
	var (
		plan syncPlan

		srcBlobs     int
		totalSrcSize int64
	)

	dstMetadata, err := c.listDestinationBlobs(ctx, dst)
	if err != nil {
		return nil, err
	}

	c.beginSyncProgress()

	if err := src.ListBlobs(ctx, "", func(srcmd blob.Metadata) error {
		totalSrcSize += srcmd.Length

		dstmd, exists := dstMetadata[srcmd.BlobID]
		delete(dstMetadata, srcmd.BlobID)

		switch {
		case !exists:
			plan.blobsToCopy = append(plan.blobsToCopy, srcmd)
			plan.totalCopyBytes += srcmd.Length
		case srcmd.Timestamp.After(dstmd.Timestamp) && c.repositorySyncUpdate:
			plan.blobsToCopy = append(plan.blobsToCopy, srcmd)
			plan.totalCopyBytes += srcmd.Length
		default:
			plan.inSyncBlobs++
			plan.inSyncBytes += srcmd.Length

			if shouldExtendObjectLock(extendLocks, srcmd.BlobID) {
				plan.blobsToExtend = append(plan.blobsToExtend, srcmd)
			}
		}

		srcBlobs++
		c.outputSyncProgress(fmt.Sprintf("  Found %v BLOBs (%v) in the source repository, %v (%v) to copy", srcBlobs, units.BytesString(totalSrcSize), len(plan.blobsToCopy), units.BytesString(plan.totalCopyBytes)))

		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "error listing blobs")
	}

	c.finishSyncProcess()

	if c.repositorySyncDelete {
		for _, dstmd := range dstMetadata {
			// found in dst, not in src since we were deleting from dst as we found a match.
			plan.blobsToDelete = append(plan.blobsToDelete, dstmd)
			plan.totalDeleteBytes += dstmd.Length
		}
	}

	return &plan, nil
}

// setupDestinationRetention validates the retention flags and, when retention
// is enabled, wraps the destination storage so that locking-prefixed blobs are
// written with the configured retention options.
func (c *commandRepositorySyncTo) setupDestinationRetention(ctx context.Context, dst blob.Storage) (blob.Storage, format.BlobStorageConfiguration, error) {
	blobcfg := format.BlobStorageConfiguration{
		RetentionMode:   blob.RetentionMode(c.repositorySyncRetentionMode),
		RetentionPeriod: c.repositorySyncRetentionPeriod,
	}
	if err := blobcfg.Validate(); err != nil {
		return dst, blobcfg, errors.Wrap(err, "invalid retention flags")
	}

	if blobcfg.IsRetentionEnabled() {
		dst = repo.WrapLockingStorage(dst, blobcfg)

		if c.repositorySyncDelete {
			log(ctx).Warn("--delete with object-lock retention may create delete markers; locked historical versions remain retained but point-in-time recovery may be required")
		}
	}

	return dst, blobcfg, nil
}

// extendLocksEnabled returns whether object locks on in-sync destination blobs
// should be extended during this run.
func (c *commandRepositorySyncTo) extendLocksEnabled(blobcfg format.BlobStorageConfiguration) bool {
	return blobcfg.IsRetentionEnabled() && c.repositorySyncExtendObjectLocks
}

// shouldExtendObjectLock returns whether the given in-sync blob is a candidate
// for object-lock extension.
func shouldExtendObjectLock(extendLocks bool, id blob.ID) bool {
	return extendLocks && repo.IsLockingStorageBlobID(id)
}

// extendDestinationLocks extends object locks on destination blobs that were
// already in sync and therefore not re-uploaded with fresh retention.
func (c *commandRepositorySyncTo) extendDestinationLocks(ctx context.Context, dst blob.Storage, blobsToExtend []blob.Metadata, blobcfg format.BlobStorageConfiguration) error {
	if len(blobsToExtend) == 0 {
		return nil
	}

	log(ctx).Info("Extending object locks...")

	c.beginSyncProgress()

	err := c.runExtendBlobRetention(ctx, dst, blobsToExtend, blob.ExtendOptions{
		RetentionMode:   blobcfg.RetentionMode,
		RetentionPeriod: blobcfg.RetentionPeriod,
	})

	c.finishSyncProcess()

	return err
}

func (c *commandRepositorySyncTo) listDestinationBlobs(ctx context.Context, dst blob.Storage) (map[blob.ID]blob.Metadata, error) {
	dstTotalBytes := int64(0)
	dstMetadata := map[blob.ID]blob.Metadata{}

	c.beginSyncProgress()

	if err := dst.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		dstMetadata[bm.BlobID] = bm
		dstTotalBytes += bm.Length
		c.outputSyncProgress(fmt.Sprintf("  Found %v BLOBs in the destination repository (%v)", len(dstMetadata), units.BytesString(dstTotalBytes)))
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "error listing BLOBs in destination repository")
	}

	c.finishSyncProcess()

	return dstMetadata, nil
}

func (c *commandRepositorySyncTo) beginSyncProgress() {
	c.lastSyncProgress = ""

	c.nextSyncOutputTime.Reset()
}

func (c *commandRepositorySyncTo) outputSyncProgress(s string) {
	if !c.progress.Enabled() {
		return
	}

	c.syncProgressMutex.Lock()
	defer c.syncProgressMutex.Unlock()

	if len(s) < len(c.lastSyncProgress) {
		s += strings.Repeat(" ", len(c.lastSyncProgress)-len(s))
	}

	if c.nextSyncOutputTime.ShouldOutput(syncProgressInterval) {
		c.out.printStderr("\r%v", s)
	}

	c.lastSyncProgress = s
}

func (c *commandRepositorySyncTo) finishSyncProcess() {
	if !c.progress.Enabled() {
		return
	}

	c.out.printStderr("\r%v\n", c.lastSyncProgress)
}

func (c *commandRepositorySyncTo) runSyncBlobs(ctx context.Context, src blob.Reader, dst blob.Storage, blobsToCopy, blobsToDelete []blob.Metadata, totalBytes int64) error {
	eg, ctx := errgroup.WithContext(ctx)
	copyCh := sliceToChannel(ctx, blobsToCopy)
	deleteCh := sliceToChannel(ctx, blobsToDelete)

	var progressMutex sync.Mutex

	var totalCopied stats.CountSum

	tt := timetrack.Start()

	for workerID := range c.repositorySyncParallelism {
		eg.Go(func() error {
			for m := range copyCh {
				log(ctx).Debugf("[%v] Copying %v (%v bytes)...\n", workerID, m.BlobID, m.Length)

				if err := c.syncCopyBlob(ctx, m, src, dst); err != nil {
					return errors.Wrapf(err, "error copying %v", m.BlobID)
				}

				numBlobs, bytesCopied := totalCopied.Add(m.Length)
				eta := "unknown"
				speed := "-"

				progressMutex.Lock()

				if est, ok := tt.Estimate(float64(bytesCopied), float64(totalBytes)); ok {
					eta = fmt.Sprintf("%v (%v)", est.Remaining, formatTimestamp(est.EstimatedEndTime))
					speed = units.BytesPerSecondsString(est.SpeedPerSecond)
				}

				c.outputSyncProgress(
					fmt.Sprintf("  Copied %v blobs (%v), Speed: %v, ETA: %v",
						numBlobs, units.BytesString(bytesCopied), speed, eta))

				progressMutex.Unlock()
			}

			for m := range deleteCh {
				log(ctx).Debugf("[%v] Deleting %v (%v bytes)...\n", workerID, m.BlobID, m.Length)

				if err := syncDeleteBlob(ctx, m, dst); err != nil {
					return errors.Wrapf(err, "error deleting %v", m.BlobID)
				}
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "error copying blobs")
	}

	return nil
}

func sliceToChannel(ctx context.Context, md []blob.Metadata) chan blob.Metadata {
	ch := make(chan blob.Metadata)

	go func() {
		defer close(ch)

		for _, it := range md {
			select {
			case ch <- it:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

func (c *commandRepositorySyncTo) syncCopyBlob(ctx context.Context, m blob.Metadata, src blob.Reader, dst blob.Storage) error {
	var data gather.WriteBuffer
	defer data.Close()

	if err := src.GetBlob(ctx, m.BlobID, 0, -1, &data); err != nil {
		if errors.Is(err, blob.ErrBlobNotFound) {
			log(ctx).Infof("ignoring BLOB not found: %v", m.BlobID)
			return nil
		}

		return errors.Wrapf(err, "error reading blob '%v' from source", m.BlobID)
	}

	opt := blob.PutOptions{}
	if c.repositorySyncTimes {
		opt.SetModTime = m.Timestamp
	}

	if err := dst.PutBlob(ctx, m.BlobID, data.Bytes(), opt); err != nil {
		if errors.Is(err, blob.ErrSetTimeUnsupported) {
			// run again without SetModTime, emit a warning
			opt.SetModTime = time.Time{}

			log(ctx).Warn("destination repository does not support preserving modification times")

			c.repositorySyncTimes = false

			err = dst.PutBlob(ctx, m.BlobID, data.Bytes(), opt)
		}

		if err != nil {
			return errors.Wrapf(err, "error writing blob '%v' to destination", m.BlobID)
		}
	}

	return nil
}

func syncDeleteBlob(ctx context.Context, m blob.Metadata, dst blob.Storage) error {
	err := dst.DeleteBlob(ctx, m.BlobID)

	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}

	return errors.Wrap(err, "error deleting blob")
}

func (c *commandRepositorySyncTo) runExtendBlobRetention(ctx context.Context, dst blob.Storage, blobsToExtend []blob.Metadata, opts blob.ExtendOptions) error {
	eg, ctx := errgroup.WithContext(ctx)
	extendCh := sliceToChannel(ctx, blobsToExtend)

	var totalExtended stats.CountSum

	for workerID := range c.repositorySyncParallelism {
		eg.Go(func() error {
			for m := range extendCh {
				log(ctx).Debugf("[%v] Extending object lock on %v...\n", workerID, m.BlobID)

				if err := syncExtendBlobRetention(ctx, m, dst, opts); err != nil {
					return errors.Wrapf(err, "error extending object lock on %v", m.BlobID)
				}

				numBlobs, _ := totalExtended.Add(m.Length)

				c.outputSyncProgress(fmt.Sprintf("  Extended object locks on %v of %v BLOBs", numBlobs, len(blobsToExtend)))
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "error extending object locks")
	}

	return nil
}

func syncExtendBlobRetention(ctx context.Context, m blob.Metadata, dst blob.Storage, opts blob.ExtendOptions) error {
	err := dst.ExtendBlobRetention(ctx, m.BlobID, opts)

	// tolerate blobs that disappeared between listing and extension, same as syncDeleteBlob.
	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}

	return wrapObjectLockError(errors.Wrap(err, "error extending blob retention"))
}

func wrapObjectLockError(err error) error {
	if errors.Is(err, blob.ErrUnsupportedPutBlobOption) || errors.Is(err, blob.ErrUnsupportedObjectLock) {
		return errors.Wrap(err, "destination storage does not support object-lock retention; remove --retention-mode/--retention-period or use an object-lock capable destination (e.g. s3)")
	}

	return err
}

func (c *commandRepositorySyncTo) ensureRepositoriesHaveSameFormatBlob(ctx context.Context, src blob.Reader, dst blob.Storage) error {
	var srcData gather.WriteBuffer
	defer srcData.Close()

	if err := src.GetBlob(ctx, format.KopiaRepositoryBlobID, 0, -1, &srcData); err != nil {
		return errors.Wrap(err, "error reading format blob")
	}

	var dstData gather.WriteBuffer
	defer dstData.Close()

	if err := dst.GetBlob(ctx, format.KopiaRepositoryBlobID, 0, -1, &dstData); err != nil {
		// target does not have format blob, save it there first.
		if errors.Is(err, blob.ErrBlobNotFound) {
			if c.repositorySyncDestinationMustExist {
				return errors.New("destination repository does not have a format blob")
			}

			return errors.Wrap(dst.PutBlob(ctx, format.KopiaRepositoryBlobID, srcData.Bytes(), blob.PutOptions{}), "error saving format blob")
		}

		return errors.Wrap(err, "error reading destination repository format blob")
	}

	uniqueID1, err := parseUniqueID(srcData.Bytes())
	if err != nil {
		return errors.Wrap(err, "error parsing unique ID of source repository")
	}

	uniqueID2, err := parseUniqueID(dstData.Bytes())
	if err != nil {
		return errors.Wrap(err, "error parsing unique ID of destination repository")
	}

	if uniqueID1 == uniqueID2 {
		return nil
	}

	return errors.New("destination repository contains incompatible data")
}

func parseUniqueID(r gather.Bytes) (string, error) {
	var f struct {
		UniqueID string `json:"uniqueID"`
	}

	if err := json.NewDecoder(r.Reader()).Decode(&f); err != nil {
		return "", errors.Wrap(err, "invalid JSON")
	}

	if f.UniqueID == "" {
		return "", errors.New("unique ID not found")
	}

	return f.UniqueID, nil
}
