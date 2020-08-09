package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/stats"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

var (
	repositorySyncCommand              = repositoryCommands.Command("sync-to", "Synchronizes contents of this repository to another location")
	repositorySyncUpdate               = repositorySyncCommand.Flag("update", "Whether to update blobs present in destination and source if the source is newer.").Default("true").Bool()
	repositorySyncDelete               = repositorySyncCommand.Flag("delete", "Whether to delete blobs present in destination but not source.").Bool()
	repositorySyncDryRun               = repositorySyncCommand.Flag("dry-run", "Do not perform copying.").Short('n').Bool()
	repositorySyncParallelism          = repositorySyncCommand.Flag("parallel", "Copy parallelism.").Default("1").Int()
	repositorySyncDestinationMustExist = repositorySyncCommand.Flag("must-exist", "Fail if destination does not have repository format blob.").Bool()
)

const syncProgressInterval = 300 * time.Millisecond

func runSyncWithStorage(ctx context.Context, src, dst blob.Storage) error {
	var ()

	noticeColor.Fprintf(os.Stderr, "Synchronizing repositories:\n\n  Source:      %v\n  Destination: %v\n\n", src.DisplayName(), dst.DisplayName()) //nolint:errcheck

	if !*repositorySyncDelete {
		noticeColor.Fprintf(os.Stderr, "NOTE: By default no BLOBs are deleted, pass --delete to allow it.\n\n") //nolint:errcheck
	}

	if err := ensureRepositoriesHaveSameFormatBlob(ctx, src, dst); err != nil {
		return err
	}

	printStderr("Looking for BLOBs to synchronize...\n")

	var (
		inSyncBlobs int
		inSyncBytes int64

		blobsToCopy    []blob.Metadata
		totalCopyBytes int64

		blobsToDelete    []blob.Metadata
		totalDeleteBytes int64

		srcBlobs     int
		totalSrcSize int64
	)

	dstMetadata, err := listDestinationBlobs(ctx, dst)
	if err != nil {
		return err
	}

	beginSyncProgress()

	if err := src.ListBlobs(ctx, "", func(srcmd blob.Metadata) error {
		totalSrcSize += srcmd.Length

		dstmd, exists := dstMetadata[srcmd.BlobID]
		delete(dstMetadata, srcmd.BlobID)

		switch {
		case !exists:
			blobsToCopy = append(blobsToCopy, srcmd)
			totalCopyBytes += srcmd.Length
		case srcmd.Timestamp.After(dstmd.Timestamp) && *repositorySyncUpdate:
			blobsToCopy = append(blobsToCopy, srcmd)
			totalCopyBytes += srcmd.Length
		default:
			inSyncBlobs++
			inSyncBytes += srcmd.Length
		}

		srcBlobs++
		outputSyncProgress(fmt.Sprintf("  Found %v BLOBs (%v) in the source repository, %v (%v) to copy", srcBlobs, units.BytesStringBase10(totalSrcSize), len(blobsToCopy), units.BytesStringBase10(totalCopyBytes)))

		return nil
	}); err != nil {
		return err
	}

	finishSyncProcess()

	if *repositorySyncDelete {
		for _, dstmd := range dstMetadata {
			// found in dst, not in src since we were deleting from dst as we found a match.
			blobsToDelete = append(blobsToDelete, dstmd)
			totalDeleteBytes += dstmd.Length
		}
	}

	printStderr(
		"  Found %v BLOBs to delete (%v), %v in sync (%v)\n",
		len(blobsToDelete), units.BytesStringBase10(totalDeleteBytes),
		inSyncBlobs, units.BytesStringBase10(inSyncBytes),
	)

	if *repositorySyncDryRun {
		return nil
	}

	printStderr("Copying...\n")

	beginSyncProgress()

	finalErr := runSyncBlobs(ctx, src, dst, blobsToCopy, blobsToDelete, totalCopyBytes)

	finishSyncProcess()

	return finalErr
}

func listDestinationBlobs(ctx context.Context, dst blob.Storage) (map[blob.ID]blob.Metadata, error) {
	dstTotalBytes := int64(0)
	dstMetadata := map[blob.ID]blob.Metadata{}

	beginSyncProgress()

	if err := dst.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		dstMetadata[bm.BlobID] = bm
		dstTotalBytes += bm.Length
		outputSyncProgress(fmt.Sprintf("  Found %v BLOBs in the destination repository (%v)", len(dstMetadata), units.BytesStringBase10(dstTotalBytes)))
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "error listing BLOBs in destination repository")
	}

	finishSyncProcess()

	return dstMetadata, nil
}

var (
	lastSyncProgress   string
	syncProgressMutex  sync.Mutex
	nextSyncOutputTime time.Time
)

func beginSyncProgress() {
	lastSyncProgress = ""
	nextSyncOutputTime = time.Now()
}

func outputSyncProgress(s string) {
	syncProgressMutex.Lock()
	defer syncProgressMutex.Unlock()

	if len(s) < len(lastSyncProgress) {
		s += strings.Repeat(" ", len(lastSyncProgress)-len(s))
	}

	if time.Now().After(nextSyncOutputTime) {
		printStderr("\r%v", s)

		nextSyncOutputTime = time.Now().Add(syncProgressInterval)
	}

	lastSyncProgress = s
}

func finishSyncProcess() {
	printStderr("\r%v\n", lastSyncProgress)
}

func runSyncBlobs(ctx context.Context, src, dst blob.Storage, blobsToCopy, blobsToDelete []blob.Metadata, totalBytes int64) error {
	eg, ctx := errgroup.WithContext(ctx)
	copyCh := sliceToChannel(ctx, blobsToCopy)
	deleteCh := sliceToChannel(ctx, blobsToDelete)

	var progressMutex sync.Mutex

	var totalCopied stats.CountSum

	startTime := time.Now()

	for i := 0; i < *repositorySyncParallelism; i++ {
		workerID := i

		eg.Go(func() error {
			for m := range copyCh {
				log(ctx).Debugf("[%v] Copying %v (%v bytes)...\n", workerID, m.BlobID, m.Length)
				if err := syncCopyBlob(ctx, m, src, dst); err != nil {
					return errors.Wrapf(err, "error copying %v", m.BlobID)
				}

				numBlobs, bytesCopied := totalCopied.Add(m.Length)
				progressMutex.Lock()
				percentage := float64(0)
				eta := "unknown"
				speed := "-"
				elapsedTime := time.Since(startTime)
				if totalBytes > 0 {
					percentage = hundredPercent * float64(bytesCopied) / float64(totalBytes)
					if percentage > 0 {
						totalTimeSeconds := elapsedTime.Seconds() * (hundredPercent / percentage)
						etaTime := startTime.Add(time.Duration(totalTimeSeconds) * time.Second)
						eta = fmt.Sprintf("%v (%v)", time.Until(etaTime).Round(time.Second), formatTimestamp(etaTime))
						bps := float64(bytesCopied) * 8 / elapsedTime.Seconds() //nolint:gomnd
						speed = units.BitsPerSecondsString(bps)
					}
				}

				outputSyncProgress(
					fmt.Sprintf("  Copied %v blobs (%v), Speed: %v, ETA: %v",
						numBlobs, units.BytesStringBase10(bytesCopied), speed, eta))
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

func syncCopyBlob(ctx context.Context, m blob.Metadata, src, dst blob.Storage) error {
	data, err := src.GetBlob(ctx, m.BlobID, 0, -1)
	if err != nil {
		if errors.Is(err, blob.ErrBlobNotFound) {
			log(ctx).Infof("ignoring BLOB not found: %v", m.BlobID)
			return nil
		}

		return errors.Wrapf(err, "error reading blob '%v' from source", m.BlobID)
	}

	if err := dst.PutBlob(ctx, m.BlobID, gather.FromSlice(data)); err != nil {
		return errors.Wrapf(err, "error writing blob '%v' to destination", m.BlobID)
	}

	return nil
}

func syncDeleteBlob(ctx context.Context, m blob.Metadata, dst blob.Storage) error {
	err := dst.DeleteBlob(ctx, m.BlobID)

	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}

	return err
}

func ensureRepositoriesHaveSameFormatBlob(ctx context.Context, src, dst blob.Storage) error {
	srcData, err := src.GetBlob(ctx, repo.FormatBlobID, 0, -1)
	if err != nil {
		return errors.Wrap(err, "error reading format blob")
	}

	dstData, err := dst.GetBlob(ctx, repo.FormatBlobID, 0, -1)
	if err != nil {
		// target does not have format blob, save it there first.
		if errors.Is(err, blob.ErrBlobNotFound) {
			if *repositorySyncDestinationMustExist {
				return errors.Errorf("destination repository does not have a format blob")
			}

			return dst.PutBlob(ctx, repo.FormatBlobID, gather.FromSlice(srcData))
		}

		return errors.Wrap(err, "error reading destination repository format blob")
	}

	if bytes.Equal(srcData, dstData) {
		return nil
	}

	return errors.Errorf("destination repository contains incompatible data")
}
