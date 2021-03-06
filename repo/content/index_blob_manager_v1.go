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
	"github.com/kopia/kopia/repo/logging"
)

type indexBlobManagerV1 struct {
	st             blob.Storage
	enc            *encryptedBlobMgr
	epochMgr       *epoch.Manager
	timeNow        func() time.Time
	log            logging.Logger
	maxPackSize    int
	indexVersion   int
	indexShardSize int
}

func (m *indexBlobManagerV1) listActiveIndexBlobs(ctx context.Context) ([]IndexBlobInfo, error) {
	active, err := m.epochMgr.GetCompleteIndexSet(ctx, epoch.LatestEpoch)
	if err != nil {
		return nil, errors.Wrap(err, "error getting index set")
	}

	var result []IndexBlobInfo

	for _, bm := range active {
		result = append(result, IndexBlobInfo{Metadata: bm})
	}

	m.log.Errorf("active indexes %v", blob.IDsFromMetadata(active))

	return result, nil
}

func (m *indexBlobManagerV1) flushCache(ctx context.Context) {
	if err := m.st.FlushCaches(ctx); err != nil {
		m.log.Debugf("error flushing caches: %v", err)
	}
}

func (m *indexBlobManagerV1) compact(ctx context.Context, opt CompactOptions) error {
	return nil
}

func (m *indexBlobManagerV1) compactEpoch(ctx context.Context, blobIDs []blob.ID, outputPrefix blob.ID) error {
	tmpbld := make(packIndexBuilder)

	for _, indexBlob := range blobIDs {
		if err := addIndexBlobsToBuilder(ctx, m.enc, tmpbld, indexBlob); err != nil {
			return errors.Wrap(err, "error adding index to builder")
		}
	}

	dataShards, err := tmpbld.buildShards(m.indexVersion, true, m.indexShardSize)
	if err != nil {
		return errors.Wrap(err, "unable to build index dataShards")
	}

	var rnd [8]byte

	if _, err := rand.Read(rnd[:]); err != nil {
		return errors.Wrap(err, "error getting random session ID")
	}

	sessionID := fmt.Sprintf("s%x-c%v", rnd[:], len(dataShards))

	for _, data := range dataShards {
		blobID, data2, err := m.enc.crypter.EncryptBLOB(data, outputPrefix, SessionID(sessionID))
		if err != nil {
			return errors.Wrap(err, "error encrypting")
		}

		if err := m.st.PutBlob(ctx, blobID, gather.FromSlice(data2)); err != nil {
			return errors.Wrap(err, "error writing index blob")
		}
	}

	return nil
}

func (m *indexBlobManagerV1) writeIndexBlobs(ctx context.Context, dataShards [][]byte, sessionID SessionID) ([]blob.Metadata, error) {
	shards := map[blob.ID]blob.Bytes{}

	sessionID = SessionID(fmt.Sprintf("%v-c%v", sessionID, len(dataShards)))

	for _, data := range dataShards {
		unprefixedBlobID, data2, err := m.enc.crypter.EncryptBLOB(data, "", sessionID)
		if err != nil {
			return nil, errors.Wrap(err, "error encrypting")
		}

		shards[unprefixedBlobID] = gather.FromSlice(data2)
	}

	// nolint:wrapcheck
	return m.epochMgr.WriteIndex(ctx, shards)
}

var _ indexBlobManager = (*indexBlobManagerV1)(nil)

// PrepareUpgradeToIndexBlobManagerV1 prepares the repository for migrating to IndexBlobManagerV1.
func (sm *SharedManager) PrepareUpgradeToIndexBlobManagerV1(ctx context.Context, params epoch.Parameters) error {
	sm.indexBlobManagerV1.epochMgr.Params = params

	ibl, err := sm.indexBlobManagerV0.listActiveIndexBlobs(ctx)
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

	sm.indexBlobManager = sm.indexBlobManagerV1

	return nil
}
