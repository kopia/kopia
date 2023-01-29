package maintenance

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

// ExtendBlobRetentionTimeOptions provides option for iextending blob retention algorithm.
type ExtendBlobRetentionTimeOptions struct {
	Parallel int
	DryRun   bool
}

// ExtendBlobRetentionTime extends the retention time of all relevant blobs managed by storage engine with Object Locking enabled.
func ExtendBlobRetentionTime(ctx context.Context, rep repo.DirectRepositoryWriter, opt ExtendBlobRetentionTimeOptions, safety SafetyParameters) (int, error) {
	const extendQueueSize = 100

	var (
		wg        sync.WaitGroup
		prefixes  []blob.ID
		cnt       = new(uint32)
		toExtend  = new(uint32)
		failedCnt = new(uint32)
	)

	if opt.Parallel == 0 {
		opt.Parallel = 16
	}

	blobCfg, err := rep.FormatManager().BlobCfgBlob()
	if err != nil {
		return 0, errors.Wrap(err, "blob configuration")
	}

	if !blobCfg.IsRetentionEnabled() {
		// Blob retention is disabled
		log(ctx).Info("Object lock retention is disabled.")
		return 0, nil
	}

	extend := make(chan blob.Metadata, extendQueueSize)
	extendOpts := blob.ExtendOptions{
		RetentionMode:   blobCfg.RetentionMode,
		RetentionPeriod: blobCfg.RetentionPeriod,
	}

	if !opt.DryRun {
		// start goroutines to extend blob retention as they come.
		for i := 0; i < opt.Parallel; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for bm := range extend {
					if err = rep.BlobStorage().ExtendBlobRetention(ctx, bm.BlobID, extendOpts); err != nil {
						log(ctx).Errorf("Failed to extend blob %v: %v", bm.BlobID, err)
						atomic.AddUint32(failedCnt, 1)

						continue
					}

					curCnt := atomic.AddUint32(cnt, 1)
					if curCnt%100 == 0 {
						log(ctx).Infof("  extended %v blobs", curCnt)
					}
				}
			}()
		}
	}

	// Convert prefixes from strong to BlobID.
	for _, pfx := range repo.GetLockingStoragePrefixes() {
		prefixes = append(prefixes, blob.ID(pfx))
	}

	// iterate all relevant (active, extendable) blobs and count them + optionally send to the channel to be extended
	log(ctx).Infof("Extending retention time for blobs...")

	err = blob.IterateAllPrefixesInParallel(ctx, opt.Parallel, rep.BlobStorage(), prefixes, func(bm blob.Metadata) error {
		if !opt.DryRun {
			extend <- bm
		}
		atomic.AddUint32(toExtend, 1)
		return nil
	})

	close(extend)
	log(ctx).Infof("Found %v blobs to extend", *toExtend)

	// wait for all extend workers to finish.
	wg.Wait()

	if *failedCnt > 0 {
		return 0, errors.Errorf("Failed to extend %v blobs", *failedCnt)
	}

	if err != nil {
		return 0, errors.Wrap(err, "error iterating packs")
	}

	if opt.DryRun {
		return int(*toExtend), nil
	}

	log(ctx).Infof("Extended total %v blobs", *cnt)

	return int(*cnt), nil
}
