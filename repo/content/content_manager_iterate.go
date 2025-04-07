package content

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/bigmap"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
)

// IterateOptions contains the options used for iterating over content.
type IterateOptions struct {
	Range          IDRange
	IncludeDeleted bool
	Parallel       int
}

// IterateCallback is the function type used as a callback during content iteration.
type (
	IterateCallback   func(Info) error
	cancelIterateFunc func() error
)

func maybeParallelExecutor(parallel int, originalCallback IterateCallback) (IterateCallback, cancelIterateFunc) {
	if parallel <= 1 {
		return originalCallback, func() error { return nil }
	}

	workch := make(chan Info, parallel)
	workererrch := make(chan error, 1)

	var wg sync.WaitGroup

	var once sync.Once

	lastWorkerError := func() error {
		select {
		case err := <-workererrch:
			return err
		default:
			return nil
		}
	}

	cleanup := func() error {
		once.Do(func() {
			close(workch)
			wg.Wait()
		})

		return lastWorkerError()
	}

	callback := func(i Info) error {
		workch <- i
		return lastWorkerError()
	}

	// start N workers, each fetching from the shared channel and invoking the provided callback.
	// cleanup() must be called to for worker completion
	for range parallel {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := range workch {
				if err := originalCallback(i); err != nil {
					select {
					case workererrch <- err:
					default:
					}
				}
			}
		}()
	}

	return callback, cleanup
}

func (bm *WriteManager) snapshotUncommittedItems(ctx context.Context) index.Builder {
	bm.lock()
	defer bm.unlock(ctx)

	overlay := bm.packIndexBuilder.Clone()

	for _, pp := range bm.pendingPacks {
		for _, pi := range pp.currentPackItems {
			overlay.Add(pi)
		}
	}

	for _, pp := range bm.writingPacks {
		for _, pi := range pp.currentPackItems {
			overlay.Add(pi)
		}
	}

	return overlay
}

// IterateContents invokes the provided callback for each content starting with a specified prefix
// and possibly including deleted items.
func (bm *WriteManager) IterateContents(ctx context.Context, opts IterateOptions, callback IterateCallback) error {
	if opts.Range == (IDRange{}) {
		// range not specified - default to AllIDs
		opts.Range = index.AllIDs
	}

	callback, cleanup := maybeParallelExecutor(opts.Parallel, callback)
	defer cleanup() //nolint:errcheck

	uncommitted := bm.snapshotUncommittedItems(ctx)

	invokeCallback := func(i Info) error {
		if !opts.IncludeDeleted {
			if ci, ok := uncommitted[i.ContentID]; ok {
				if ci.Deleted {
					return nil
				}
			} else if i.Deleted {
				return nil
			}
		}

		if !opts.Range.Contains(i.ContentID) {
			return nil
		}

		return callback(i)
	}

	if len(uncommitted) == 0 && opts.IncludeDeleted && opts.Range == index.AllIDs && opts.Parallel <= 1 {
		// fast path, invoke callback directly
		invokeCallback = callback
	}

	for _, bi := range uncommitted {
		_ = invokeCallback(bi)
	}

	if err := bm.maybeRefreshIndexes(ctx); err != nil {
		return err
	}

	if err := bm.committedContents.listContents(opts.Range, invokeCallback); err != nil {
		return err
	}

	return cleanup()
}

// IteratePackOptions are the options used to iterate over packs.
type IteratePackOptions struct {
	IncludePacksWithOnlyDeletedContent bool
	IncludeContentInfos                bool
	Prefixes                           []blob.ID
}

func (o *IteratePackOptions) matchesBlob(id blob.ID) bool {
	if len(o.Prefixes) == 0 {
		return true
	}

	for _, p := range o.Prefixes {
		if strings.HasPrefix(string(id), string(p)) {
			return true
		}
	}

	return false
}

// PackInfo contains the data for a pack.
type PackInfo struct {
	PackID       blob.ID
	ContentCount int
	TotalSize    int64
	ContentInfos []Info
}

// IteratePacksCallback is the function type used as callback during pack iteration.
type IteratePacksCallback func(PackInfo) error

// IteratePacks invokes the provided callback for all pack blobs.
func (bm *WriteManager) IteratePacks(ctx context.Context, options IteratePackOptions, callback IteratePacksCallback) error {
	packUsage := map[blob.ID]*PackInfo{}

	if err := bm.IterateContents(
		ctx,
		IterateOptions{
			IncludeDeleted: options.IncludePacksWithOnlyDeletedContent,
		},
		func(ci Info) error {
			if !options.matchesBlob(ci.PackBlobID) {
				return nil
			}

			pi := packUsage[ci.PackBlobID]
			if pi == nil {
				pi = &PackInfo{}
				packUsage[ci.PackBlobID] = pi
			}
			pi.PackID = ci.PackBlobID
			pi.ContentCount++
			pi.TotalSize += int64(ci.PackedLength)
			if options.IncludeContentInfos {
				pi.ContentInfos = append(pi.ContentInfos, ci)
			}
			return nil
		}); err != nil {
		return errors.Wrap(err, "error iterating contents")
	}

	for _, v := range packUsage {
		if err := callback(*v); err != nil {
			return err
		}
	}

	return nil
}

// IterateUnreferencedBlobs returns the list of unreferenced storage blobs.
func (bm *WriteManager) IterateUnreferencedBlobs(ctx context.Context, blobPrefixes []blob.ID, parallellism int, callback func(blob.Metadata) error) error {
	usedPacks, err := bigmap.NewSet(ctx)
	if err != nil {
		return errors.Wrap(err, "new set")
	}

	defer usedPacks.Close(ctx)

	bm.log.Debug("determining blobs in use")
	// find packs in use
	if err := bm.IteratePacks(
		ctx,
		IteratePackOptions{
			Prefixes:                           blobPrefixes,
			IncludePacksWithOnlyDeletedContent: true,
		},
		func(pi PackInfo) error {
			if pi.ContentCount > 0 {
				usedPacks.Put(ctx, []byte(pi.PackID))
			}
			return nil
		}); err != nil {
		return errors.Wrap(err, "error iterating packs")
	}

	unusedCount := new(int32)

	if len(blobPrefixes) == 0 {
		blobPrefixes = PackBlobIDPrefixes
	}

	var prefixes []blob.ID

	if parallellism <= len(blobPrefixes) {
		prefixes = append(prefixes, blobPrefixes...)
	} else {
		// iterate {p,q}[0-9,a-f]
		for _, prefix := range blobPrefixes {
			for hexDigit := range 16 {
				prefixes = append(prefixes, blob.ID(fmt.Sprintf("%v%x", prefix, hexDigit)))
			}
		}
	}

	bm.log.Debugf("scanning prefixes %v", prefixes)

	if err := blob.IterateAllPrefixesInParallel(ctx, parallellism, bm.st, prefixes,
		func(bm blob.Metadata) error {
			if usedPacks.Contains([]byte(bm.BlobID)) {
				return nil
			}

			atomic.AddInt32(unusedCount, 1)

			return callback(bm)
		}); err != nil {
		return errors.Wrap(err, "error iterating blobs")
	}

	bm.log.Debugf("found %v pack blobs not in use", *unusedCount)

	return nil
}
