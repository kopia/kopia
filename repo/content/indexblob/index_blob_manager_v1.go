package indexblob

import (
	"context"
	"crypto/rand"
	stderrors "errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobcrypto"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/maintenancestats"
)

var compactionLog = logging.Module("index-compaction")

// ManagerV1 is the append-only implementation of indexblob.Manager
// based on epoch manager.
type ManagerV1 struct {
	st                    blob.Storage
	enc                   *EncryptionManager
	timeNow               func() time.Time
	formattingOptions     IndexFormattingOptions
	log                   *contentlog.Logger
	cacheDirectory        string
	compactionMemoryLimit int64

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

	var result []Metadata

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
	if m.cacheDirectory == "" {
		return m.compactEpochInMemory(ctx, blobIDs, outputPrefix)
	}

	opt := epochCompactionOptionsFromContext(ctx, m.compactionMemoryLimit)

	var retryable bool

	attempt := 0
	err := retry.WithExponentialBackoffNoValue(ctx, "compact indexes after temporary file disappearance", func() error {
		if attempt > 0 {
			compactionLog(ctx).Infof("Retrying index compaction (retry %v) after temporary files disappeared; replaying the original inputs...", attempt)
		}

		attempt++

		var err error

		retryable, err = m.compactEpochWithSpilling(ctx, blobIDs, outputPrefix, opt)
		if ctx.Err() != nil {
			retryable = false
			return stderrors.Join(err, ctx.Err())
		}

		return err
	}, func(error) bool {
		return retryable && ctx.Err() == nil
	})

	return err
}

func (m *ManagerV1) compactEpochWithSpilling(ctx context.Context, blobIDs []blob.ID, outputPrefix blob.ID, opt EpochCompactionOptions) (retryable bool, retErr error) {
	var (
		tmpbld *index.SpillingBuilder
		shards *compactionShardStore
	)

	defer func() {
		// Only local scratch-file disappearance permits replay. In particular,
		// a missing repository blob must not be mistaken for a cleared cache.
		retryable = stderrors.Is(retErr, index.ErrSpillingFileMissing) || stderrors.Is(retErr, errCompactionTemporaryDirectoryMissing)

		var cleanupErr error
		if shards != nil {
			cleanupErr = shards.close()
		}

		if tmpbld != nil {
			cleanupErr = stderrors.Join(cleanupErr, tmpbld.Close())
		}

		if cleanupErr != nil {
			retryable = false
			retErr = stderrors.Join(retErr, cleanupErr)
		}
	}()

	if err := ctx.Err(); err != nil {
		return false, errors.Wrap(err, "index compaction canceled")
	}

	tempDirectory, err := m.compactionTempDirectory()
	if err != nil {
		return false, err
	}

	tmpbld, err = index.NewSpillingBuilder(opt.MemoryLimitBytes, tempDirectory)
	if err != nil {
		return false, errors.Wrap(err, "creating index compaction builder")
	}

	contentlog.Log2(ctx, m.log, "compacting indexes with bounded working memory",
		logparam.Int("inputBlobs", len(blobIDs)), logparam.Int64("memoryLimitBytes", opt.MemoryLimitBytes))
	compactionLog(ctx).Infof("Compacting %v index blobs with a %v working-memory budget...", len(blobIDs), units.BytesString(opt.MemoryLimitBytes))

	for _, indexBlob := range blobIDs {
		if err := iterateIndexBlob(ctx, m.enc, indexBlob, func(i index.Info) error {
			return tmpbld.Add(ctx, i)
		}); err != nil {
			return false, errors.Wrap(err, "error adding index to builder")
		}
	}

	mp, mperr := m.formattingOptions.GetMutableParameters(ctx)
	if mperr != nil {
		return false, errors.Wrap(mperr, "mutable parameters")
	}

	shards = &compactionShardStore{tempDirectory: tempDirectory}

	compactionLog(ctx).Info("Merging indexes and preparing output shards...")

	if err := tmpbld.BuildShards(ctx, mp.IndexVersion, DefaultIndexShardSize, func(data gather.Bytes) error {
		return shards.add(ctx, data)
	}); err != nil {
		return false, errors.Wrap(err, "unable to build index shards")
	}

	// All input has been read successfully. Release merge files before uploading.
	if err := tmpbld.Close(); err != nil {
		return false, errors.Wrap(err, "closing index compaction builder")
	}

	return false, m.publishCompactedShards(ctx, outputPrefix, shards.count, shards.replay)
}

// compactEpochInMemory preserves disk-free compaction when local caching is disabled.
func (m *ManagerV1) compactEpochInMemory(ctx context.Context, blobIDs []blob.ID, outputPrefix blob.ID) error {
	builder := index.NewOneUseBuilder()
	for _, id := range blobIDs {
		if err := addIndexBlobsToBuilder(ctx, m.enc, builder.Add, id); err != nil {
			return errors.Wrap(err, "error adding index to builder")
		}
	}

	mp, err := m.formattingOptions.GetMutableParameters(ctx)
	if err != nil {
		return errors.Wrap(err, "mutable parameters")
	}

	shards, cleanup, err := builder.BuildShards(mp.IndexVersion, true, DefaultIndexShardSize)
	if err != nil {
		return errors.Wrap(err, "unable to build index shards")
	}
	defer cleanup()

	return m.publishCompactedShards(ctx, outputPrefix, len(shards), func(ctx context.Context, emit func(gather.Bytes) error) error {
		for _, shard := range shards {
			if err := ctx.Err(); err != nil {
				return errors.Wrap(err, "publishing index compaction shards canceled")
			}

			if err := emit(shard); err != nil {
				return err
			}
		}

		return nil
	})
}

func (m *ManagerV1) publishCompactedShards(ctx context.Context, outputPrefix blob.ID, count int, replay func(context.Context, func(gather.Bytes) error) error) error {
	if count == 0 {
		return nil
	}

	var rnd [8]byte

	if _, err := rand.Read(rnd[:]); err != nil {
		return errors.Wrap(err, "error getting random session ID")
	}

	// Every shard belongs to the same complete set, even when it was built in a
	// different spill/merge batch. Partial uploads must never supersede inputs.
	sessionID := fmt.Sprintf("s%x-c%v", rnd[:], count)
	compactionLog(ctx).Infof("Publishing %v compacted index shards...", count)

	var data2 gather.WriteBuffer
	defer data2.Close()

	err := replay(ctx, func(data gather.Bytes) error {
		data2.Reset()

		blobID, err := blobcrypto.Encrypt(m.enc.crypter, data, outputPrefix, blob.ID(sessionID), &data2)
		if err != nil {
			return errors.Wrap(err, "error encrypting")
		}

		if err := m.st.PutBlob(ctx, blobID, data2.Bytes(), blob.PutOptions{}); err != nil {
			return errors.Wrap(err, "error writing index blob")
		}

		return nil
	})

	return errors.Wrap(err, "publishing compacted index shards")
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

	var blobIDs []blob.ID

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
	cacheDirectory string,
	compactionMemoryLimit int64,
) *ManagerV1 {
	return &ManagerV1{
		st:                    st,
		enc:                   enc,
		timeNow:               timeNow,
		log:                   log,
		formattingOptions:     formattingOptions,
		cacheDirectory:        cacheDirectory,
		compactionMemoryLimit: compactionMemoryLimit,

		epochMgr: epochMgr,
	}
}

var _ Manager = (*ManagerV1)(nil)

// compactionTempDirectory keeps scratch files separate from managed cache
// entries. It is only used when a local cache directory is configured.
func (m *ManagerV1) compactionTempDirectory() (string, error) {
	if m.cacheDirectory == "" {
		return "", errors.New("index compaction temporary files require a cache directory")
	}

	dir := filepath.Join(m.cacheDirectory, "index-compaction")
	if err := os.MkdirAll(dir, cache.DirMode); err != nil {
		return "", errors.Wrap(compactionTemporaryFileError(err), "creating index compaction temporary directory")
	}

	return dir, nil
}
