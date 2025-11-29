package indexblob

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobcrypto"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/maintenancestats"
)

// ManagerV1 is the append-only implementation of indexblob.Manager
// based on epoch manager.
type ManagerV1 struct {
	st                blob.Storage
	enc               *EncryptionManager
	timeNow           func() time.Time
	formattingOptions IndexFormattingOptions
	log               *contentlog.Logger

	epochMgr *epoch.Manager
}

// ListIndexBlobInfos lists active blob info structs.
func (m *ManagerV1) ListIndexBlobInfos(ctx context.Context) ([]Metadata, error) {
	blobs, _, err := m.ListActiveIndexBlobs(ctx)

	return blobs, err
}

// ListActiveIndexBlobs lists the metadata for active index blobs and returns the cut-off time
// before which all deleted index entries should be treated as non-existent.
func (m *ManagerV1) ListActiveIndexBlobs(ctx context.Context) ([]Metadata, time.Time, error) {
	active, deletionWatermark, err := m.epochMgr.GetCompleteIndexSet(ctx, epoch.LatestEpoch)
	if err != nil {
		return nil, time.Time{}, errors.Wrap(err, "error getting index set")
	}

	result := make([]Metadata, 0, len(active))

	for _, bm := range active {
		result = append(result, Metadata{Metadata: bm})
	}

	contentlog.Log2(ctx, m.log, "total active indexes", logparam.Int("len", len(active)), logparam.Time("deletionWatermark", deletionWatermark))

	return result, deletionWatermark, nil
}

// Invalidate clears any read caches.
func (m *ManagerV1) Invalidate() {
	m.epochMgr.Invalidate()
}

// Compact advances the deletion watermark.
func (m *ManagerV1) Compact(ctx context.Context, opt CompactOptions) (*maintenancestats.CompactIndexesStats, error) {
	if opt.DropDeletedBefore.IsZero() {
		return nil, nil
	}

	advanced, err := m.epochMgr.AdvanceDeletionWatermark(ctx, opt.DropDeletedBefore)
	if err != nil {
		return nil, errors.Wrap(err, "error advancing deletion watermark")
	}

	if !advanced {
		return nil, nil
	}

	return &maintenancestats.CompactIndexesStats{
		DroppedContentsDeletedBefore: opt.DropDeletedBefore,
	}, nil
}

// CompactEpoch compacts the provided index blobs and writes a new set of blobs.
func (m *ManagerV1) CompactEpoch(ctx context.Context, blobIDs []blob.ID, outputPrefix blob.ID) error {
	tmpbld := index.NewOneUseBuilder()

	for _, indexBlob := range blobIDs {
		if err := addIndexBlobsToBuilder(ctx, m.enc, tmpbld.Add, indexBlob); err != nil {
			return errors.Wrap(err, "error adding index to builder")
		}
	}

	mp, mperr := m.formattingOptions.GetMutableParameters(ctx)
	if mperr != nil {
		return errors.Wrap(mperr, "mutable parameters")
	}

	dataShards, cleanupShards, err := tmpbld.BuildShards(mp.IndexVersion, true, DefaultIndexShardSize)
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

		blobID, err := blobcrypto.Encrypt(m.enc.crypter, data, outputPrefix, blob.ID(sessionID), &data2)
		if err != nil {
			return errors.Wrap(err, "error encrypting")
		}

		if err := m.st.PutBlob(ctx, blobID, data2.Bytes(), blob.PutOptions{}); err != nil {
			return errors.Wrap(err, "error writing index blob")
		}
	}

	return nil
}

// WriteIndexBlobs writes dataShards into new index blobs with an optional blob name suffix.
// The writes are atomic in the sense that if any of them fails, the reader will
// ignore all of the indexes that share the same suffix.
func (m *ManagerV1) WriteIndexBlobs(ctx context.Context, dataShards []gather.Bytes, suffix blob.ID) ([]blob.Metadata, error) {
	shards := map[blob.ID]blob.Bytes{}

	suffix = blob.ID(fmt.Sprintf("%v-c%v", suffix, len(dataShards)))

	for _, data := range dataShards {
		// important - we're intentionally using data2 in the inner loop scheduling multiple Close()
		// we want all Close() to be called at the end of the function after WriteIndex()
		data2 := gather.NewWriteBuffer()
		defer data2.Close() //nolint:gocritic

		unprefixedBlobID, err := blobcrypto.Encrypt(m.enc.crypter, data, "", suffix, data2)
		if err != nil {
			return nil, errors.Wrap(err, "error encrypting")
		}

		shards[unprefixedBlobID] = data2.Bytes()
	}

	//nolint:wrapcheck
	return m.epochMgr.WriteIndex(ctx, shards)
}

// EpochManager returns the epoch manager.
func (m *ManagerV1) EpochManager() *epoch.Manager {
	return m.epochMgr
}

// PrepareUpgradeToIndexBlobManagerV1 prepares the repository for migrating to IndexBlobManagerV1.
func (m *ManagerV1) PrepareUpgradeToIndexBlobManagerV1(ctx context.Context, v0 *ManagerV0) error {
	ibl, _, err := v0.ListActiveIndexBlobs(ctx)
	if err != nil {
		return errors.Wrap(err, "error listing active index blobs")
	}

	blobIDs := make([]blob.ID, 0, len(ibl))

	for _, ib := range ibl {
		blobIDs = append(blobIDs, ib.BlobID)
	}

	if err := m.CompactEpoch(ctx, blobIDs, epoch.UncompactedEpochBlobPrefix(epoch.FirstEpoch)); err != nil {
		return errors.Wrap(err, "unable to generate initial epoch")
	}

	return nil
}

// NewManagerV1 creates new instance of ManagerV1 with all required parameters set.
func NewManagerV1(
	st blob.Storage,
	enc *EncryptionManager,
	epochMgr *epoch.Manager,
	timeNow func() time.Time,
	formattingOptions IndexFormattingOptions,
	log *contentlog.Logger,
) *ManagerV1 {
	return &ManagerV1{
		st:                st,
		enc:               enc,
		timeNow:           timeNow,
		log:               log,
		formattingOptions: formattingOptions,

		epochMgr: epochMgr,
	}
}

var _ Manager = (*ManagerV1)(nil)
