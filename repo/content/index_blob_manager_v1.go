package content

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/logging"
)

type indexBlobManagerV1 struct {
	st                blob.Storage
	enc               *encryptedBlobMgr
	epochMgr          *epoch.Manager
	timeNow           func() time.Time
	log               logging.Logger
	formattingOptions IndexFormattingOptions
}

func (m *indexBlobManagerV1) listActiveIndexBlobs(ctx context.Context) ([]IndexBlobInfo, time.Time, error) {
	active, deletionWatermark, err := m.epochMgr.GetCompleteIndexSet(ctx, epoch.LatestEpoch)
	if err != nil {
		return nil, time.Time{}, errors.Wrap(err, "error getting index set")
	}

	var result []IndexBlobInfo

	for _, bm := range active {
		result = append(result, IndexBlobInfo{Metadata: bm})
	}

	m.log.Errorf("active indexes %v deletion watermark %v", blob.IDsFromMetadata(active), deletionWatermark)

	return result, deletionWatermark, nil
}

func (m *indexBlobManagerV1) invalidate(ctx context.Context) {
	m.epochMgr.Invalidate()
}

func (m *indexBlobManagerV1) flushCache(ctx context.Context) {
	if err := m.st.FlushCaches(ctx); err != nil {
		m.log.Debugf("error flushing caches: %v", err)
	}
}

func (m *indexBlobManagerV1) compact(ctx context.Context, opt CompactOptions) error {
	if opt.DropDeletedBefore.IsZero() {
		return nil
	}

	return errors.Wrap(m.epochMgr.AdvanceDeletionWatermark(ctx, opt.DropDeletedBefore), "error advancing deletion watermark")
}

func (m *indexBlobManagerV1) compactEpoch(ctx context.Context, blobIDs []blob.ID, outputPrefix blob.ID) error {
	tmpbld := make(index.Builder)

	for _, indexBlob := range blobIDs {
		if err := addIndexBlobsToBuilder(ctx, m.enc, tmpbld, indexBlob); err != nil {
			return errors.Wrap(err, "error adding index to builder")
		}
	}

	dataShards, cleanupShards, err := tmpbld.BuildShards(m.formattingOptions.WriteIndexVersion(), true, m.formattingOptions.IndexShardSize())
	if err != nil {
		return errors.Wrap(err, "unable to build index dataShards")
	}

	defer cleanupShards()

	var rnd [8]byte

	if _, err := rand.Read(rnd[:]); err != nil {
		return errors.Wrap(err, "error getting random session ID")
	}

	sessionID := fmt.Sprintf("s%x-c%v", rnd[:], len(dataShards))

	var data2 gather.WriteBuffer
	defer data2.Close()

	for _, data := range dataShards {
		data2.Reset()

		blobID, err := EncryptBLOB(m.enc.crypter, data, outputPrefix, SessionID(sessionID), &data2)
		if err != nil {
			return errors.Wrap(err, "error encrypting")
		}

		if err := m.st.PutBlob(ctx, blobID, data2.Bytes(), blob.PutOptions{}); err != nil {
			return errors.Wrap(err, "error writing index blob")
		}
	}

	return nil
}

func (m *indexBlobManagerV1) writeIndexBlobs(ctx context.Context, dataShards []gather.Bytes, sessionID SessionID) ([]blob.Metadata, error) {
	shards := map[blob.ID]blob.Bytes{}

	sessionID = SessionID(fmt.Sprintf("%v-c%v", sessionID, len(dataShards)))

	for _, data := range dataShards {
		// important - we're intentionally using data2 in the inner loop scheduling multiple Close()
		// we want all Close() to be called at the end of the function after WriteIndex()
		data2 := gather.NewWriteBuffer()
		defer data2.Close() //nolint:gocritic

		unprefixedBlobID, err := EncryptBLOB(m.enc.crypter, data, "", sessionID, data2)
		if err != nil {
			return nil, errors.Wrap(err, "error encrypting")
		}

		shards[unprefixedBlobID] = data2.Bytes()
	}

	// nolint:wrapcheck
	return m.epochMgr.WriteIndex(ctx, shards)
}

var _ indexBlobManager = (*indexBlobManagerV1)(nil)

// PrepareUpgradeToIndexBlobManagerV1 prepares the repository for migrating to IndexBlobManagerV1.
func (sm *SharedManager) PrepareUpgradeToIndexBlobManagerV1(ctx context.Context, params epoch.Parameters) error {
	ibl, _, err := sm.indexBlobManagerV0.listActiveIndexBlobs(ctx)
	if err != nil {
		return errors.Wrap(err, "error listing active index blobs")
	}

	var blobIDs []blob.ID

	for _, ib := range ibl {
		blobIDs = append(blobIDs, ib.BlobID)
	}

	if err := sm.indexBlobManagerV1.compactEpoch(ctx, blobIDs, epoch.UncompactedEpochBlobPrefix(epoch.FirstEpoch)); err != nil {
		return errors.Wrap(err, "unable to generate initial epoch")
	}

	return nil
}
