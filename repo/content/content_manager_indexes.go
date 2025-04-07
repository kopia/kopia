package content

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobcrypto"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/content/indexblob"
)

// Refresh reloads the committed content indexes.
func (sm *SharedManager) Refresh(ctx context.Context) error {
	sm.indexesLock.Lock()
	defer sm.indexesLock.Unlock()

	sm.log.Debug("Refresh started")

	ibm, err := sm.indexBlobManager(ctx)
	if err != nil {
		return err
	}

	ibm.Invalidate()

	timer := timetrack.StartTimer()

	err = sm.loadPackIndexesLocked(ctx)
	sm.log.Debugf("Refresh completed in %v", timer.Elapsed())

	return err
}

// CompactIndexes performs compaction of index blobs ensuring that # of small index blobs is below opt.maxSmallBlobs.
func (sm *SharedManager) CompactIndexes(ctx context.Context, opt indexblob.CompactOptions) error {
	// we must hold the lock here to avoid the race with Refresh() which can reload the
	// current set of indexes while we process them.
	sm.indexesLock.Lock()
	defer sm.indexesLock.Unlock()

	sm.log.Debugf("CompactIndexes(%+v)", opt)

	ibm, err := sm.indexBlobManager(ctx)
	if err != nil {
		return err
	}

	if err := ibm.Compact(ctx, opt); err != nil {
		return errors.Wrap(err, "error performing compaction")
	}

	// reload indexes after compaction.
	if err := sm.loadPackIndexesLocked(ctx); err != nil {
		return errors.Wrap(err, "error re-loading indexes")
	}

	return nil
}

// ParseIndexBlob loads entries in a given index blob and returns them.
func ParseIndexBlob(blobID blob.ID, encrypted gather.Bytes, crypter blobcrypto.Crypter) ([]Info, error) {
	var data gather.WriteBuffer
	defer data.Close()

	if err := blobcrypto.Decrypt(crypter, encrypted, blobID, &data); err != nil {
		return nil, errors.Wrap(err, "unable to decrypt index blob")
	}

	ndx, err := index.Open(data.Bytes().ToByteSlice(), nil, crypter.Encryptor().Overhead)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open index blob")
	}

	var results []Info

	err = ndx.Iterate(index.AllIDs, func(i index.Info) error {
		results = append(results, i)
		return nil
	})

	return results, errors.Wrap(err, "error iterating index entries")
}
