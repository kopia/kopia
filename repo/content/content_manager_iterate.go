package content

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

// IterateOptions contains the options used for iterating over content
type IterateOptions struct {
	Prefix         ID
	IncludeDeleted bool
	Parallel       int
}

// IterateCallback is the function type used as a callback during content iteration
type IterateCallback func(Info) error
type cancelIterateFunc func() error

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
	for i := 0; i < parallel; i++ {
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

func (bm *Manager) snapshotUncommittedItems() packIndexBuilder {
	bm.lock()
	defer bm.unlock()

	overlay := bm.packIndexBuilder.clone()

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
func (bm *Manager) IterateContents(ctx context.Context, opts IterateOptions, callback IterateCallback) error {
	callback, cleanup := maybeParallelExecutor(opts.Parallel, callback)
	defer cleanup() //nolint:errcheck

	uncommitted := bm.snapshotUncommittedItems()

	invokeCallback := func(i Info) error {
		if !opts.IncludeDeleted {
			if ci, ok := uncommitted[i.ID]; ok {
				if ci.Deleted {
					return nil
				}
			} else if i.Deleted {
				return nil
			}
		}

		if !strings.HasPrefix(string(i.ID), string(opts.Prefix)) {
			return nil
		}

		return callback(i)
	}

	if len(uncommitted) == 0 && opts.IncludeDeleted && opts.Prefix == "" && opts.Parallel <= 1 {
		// fast path, invoke callback directly
		invokeCallback = callback
	}

	for _, bi := range uncommitted {
		_ = invokeCallback(*bi)
	}

	if err := bm.committedContents.listContents(opts.Prefix, invokeCallback); err != nil {
		return err
	}

	return cleanup()
}

// IteratePackOptions are the options used to iterate over packs
type IteratePackOptions struct {
	IncludePacksWithOnlyDeletedContent bool
	IncludeContentInfos                bool
}

// PackInfo contains the data for a pack
type PackInfo struct {
	PackID       blob.ID
	ContentCount int
	TotalSize    int64
	ContentInfos []Info
}

// IteratePacksCallback is the function type used as callback during pack iteration
type IteratePacksCallback func(PackInfo) error

// IteratePacks invokes the provided callback for all pack blobs.
func (bm *Manager) IteratePacks(ctx context.Context, options IteratePackOptions, callback IteratePacksCallback) error {
	packUsage := map[blob.ID]*PackInfo{}

	if err := bm.IterateContents(
		ctx,
		IterateOptions{
			IncludeDeleted: options.IncludePacksWithOnlyDeletedContent,
		},
		func(ci Info) error {
			pi := packUsage[ci.PackBlobID]
			if pi == nil {
				pi = &PackInfo{}
				packUsage[ci.PackBlobID] = pi
			}
			pi.PackID = ci.PackBlobID
			pi.ContentCount++
			pi.TotalSize += int64(ci.Length)
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

// IterateContentInShortPacks invokes the provided callback for all contents that are stored in
// packs shorter than the given threshold.
func (bm *Manager) IterateContentInShortPacks(ctx context.Context, threshold int64, callback IterateCallback) error {
	if threshold <= 0 {
		threshold = int64(bm.maxPackSize) * 8 / 10 // nolint:gomnd
	}

	return bm.IteratePacks(
		ctx,
		IteratePackOptions{
			IncludePacksWithOnlyDeletedContent: true,
			IncludeContentInfos:                true,
		},
		func(pi PackInfo) error {
			if pi.TotalSize >= threshold {
				return nil
			}

			for _, ci := range pi.ContentInfos {
				if err := callback(ci); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

// IterateUnreferencedBlobs returns the list of unreferenced storage blobs.
func (bm *Manager) IterateUnreferencedBlobs(ctx context.Context, parallellism int, callback func(blob.Metadata) error) error {
	usedPacks := map[blob.ID]bool{}

	log(ctx).Infof("determining blobs in use")
	// find packs in use
	if err := bm.IteratePacks(
		ctx,
		IteratePackOptions{
			IncludePacksWithOnlyDeletedContent: true,
		},
		func(pi PackInfo) error {
			if pi.ContentCount > 0 {
				usedPacks[pi.PackID] = true
			}
			return nil
		}); err != nil {
		return errors.Wrap(err, "error iterating packs")
	}

	log(ctx).Infof("found %v pack blobs in use", len(usedPacks))

	unusedCount := 0

	var prefixes []blob.ID

	if parallellism <= len(PackBlobIDPrefixes) {
		prefixes = append(prefixes, PackBlobIDPrefixes...)
	} else {
		// iterate {p,q}[0-9,a-f]
		for _, prefix := range PackBlobIDPrefixes {
			for hexDigit := 0; hexDigit < 16; hexDigit++ {
				prefixes = append(prefixes, blob.ID(fmt.Sprintf("%v%x", prefix, hexDigit)))
			}
		}
	}

	if err := blob.IterateAllPrefixesInParallel(ctx, parallellism, bm.st, prefixes,
		func(bm blob.Metadata) error {
			if usedPacks[bm.BlobID] {
				return nil
			}

			unusedCount++

			return callback(bm)
		}); err != nil {
		return errors.Wrap(err, "error iterating blobs")
	}

	log(ctx).Infof("found %v pack blobs not in use", unusedCount)

	return nil
}
