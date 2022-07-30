package content

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
)

const verySmallContentFraction = 20 // blobs less than 1/verySmallContentFraction of maxPackSize are considered 'very small'

// CompactOptions provides options for compaction.
type CompactOptions struct {
	MaxSmallBlobs                    int
	AllIndexes                       bool
	DropDeletedBefore                time.Time
	DropContents                     []ID
	DisableEventualConsistencySafety bool
}

func (co *CompactOptions) maxEventualConsistencySettleTime() time.Duration {
	if co.DisableEventualConsistencySafety {
		return 0
	}

	return defaultEventualConsistencySettleTime
}

// Refresh reloads the committed content indexes.
func (sm *SharedManager) Refresh(ctx context.Context) error {
	sm.indexesLock.Lock()
	defer sm.indexesLock.Unlock()

	sm.log.Debugf("Refresh started")

	sm.indexBlobManager().invalidate(ctx)

	timer := timetrack.StartTimer()

	err := sm.loadPackIndexesLocked(ctx)
	sm.log.Debugf("Refresh completed in %v", timer.Elapsed())

	return err
}

// CompactIndexes performs compaction of index blobs ensuring that # of small index blobs is below opt.maxSmallBlobs.
func (sm *SharedManager) CompactIndexes(ctx context.Context, opt CompactOptions) error {
	// we must hold the lock here to avoid the race with Refresh() which can reload the
	// current set of indexes while we process them.
	sm.indexesLock.Lock()
	defer sm.indexesLock.Unlock()

	sm.log.Debugf("CompactIndexes(%+v)", opt)

	if err := sm.indexBlobManager().compact(ctx, opt); err != nil {
		return errors.Wrap(err, "error performing compaction")
	}

	// reload indexes after compaction.
	if err := sm.loadPackIndexesLocked(ctx); err != nil {
		return errors.Wrap(err, "error re-loading indexes")
	}

	return nil
}

// ParseIndexBlob loads entries in a given index blob and returns them.
func ParseIndexBlob(ctx context.Context, blobID blob.ID, encrypted gather.Bytes, crypter crypter) ([]Info, error) {
	var data gather.WriteBuffer
	defer data.Close()

	if err := DecryptBLOB(crypter, encrypted, blobID, &data); err != nil {
		return nil, errors.Wrap(err, "unable to decrypt index blob")
	}

	ndx, err := index.Open(data.Bytes().ToByteSlice(), nil, uint32(crypter.Encryptor().Overhead()))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open index blob")
	}

	var results []Info

	err = ndx.Iterate(index.AllIDs, func(i Info) error {
		results = append(results, index.ToInfoStruct(i))
		return nil
	})

	return results, errors.Wrap(err, "error iterating index entries")
}

func addBlobsToIndex(ndx map[blob.ID]*IndexBlobInfo, blobs []blob.Metadata) {
	for _, it := range blobs {
		if ndx[it.BlobID] == nil {
			ndx[it.BlobID] = &IndexBlobInfo{
				Metadata: blob.Metadata{
					BlobID:    it.BlobID,
					Length:    it.Length,
					Timestamp: it.Timestamp,
				},
			}
		}
	}
}
