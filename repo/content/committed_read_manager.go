package content

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/cacheprot"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/listcache"
	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/internal/ownwrites"
	"github.com/kopia/kopia/internal/repodiag"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/blob/sharded"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/logging"
)

// number of bytes to read from each pack index when recovering the index.
// per-pack indexes are usually short (<100-200 contents).
const indexRecoverPostambleSize = 8192

const indexRefreshFrequency = 15 * time.Minute

const ownWritesCacheDuration = 15 * time.Minute

// constants below specify how long to prevent cache entries from expiring.
const (
	DefaultMetadataCacheSweepAge = 24 * time.Hour
	DefaultDataCacheSweepAge     = 10 * time.Minute
	DefaultIndexCacheSweepAge    = 1 * time.Hour
)

//nolint:gochecknoglobals
var cachedIndexBlobPrefixes = []blob.ID{
	indexblob.V0IndexBlobPrefix,
	indexblob.V0CompactionLogBlobPrefix,
	indexblob.V0CleanupBlobPrefix,

	epoch.UncompactedIndexBlobPrefix,
	epoch.EpochMarkerIndexBlobPrefix,
	epoch.SingleEpochCompactionBlobPrefix,
	epoch.RangeCheckpointIndexBlobPrefix,
}

//nolint:gochecknoglobals
var allIndexBlobPrefixes = []blob.ID{
	indexblob.V0IndexBlobPrefix,
	epoch.UncompactedIndexBlobPrefix,
	epoch.SingleEpochCompactionBlobPrefix,
	epoch.RangeCheckpointIndexBlobPrefix,
}

// IndexBlobReader provides an API for reading index blobs.
type IndexBlobReader interface {
	ListIndexBlobInfos(ctx context.Context) ([]indexblob.Metadata, time.Time, error)
}

// SharedManager is responsible for read-only access to committed data.
type SharedManager struct {
	Stats *Stats
	st    blob.Storage

	indexBlobManagerV0 *indexblob.ManagerV0
	indexBlobManagerV1 *indexblob.ManagerV1

	contentCache      cache.ContentCache
	metadataCache     cache.ContentCache
	indexBlobCache    *cache.PersistentCache
	committedContents *committedContentIndex
	timeNow           func() time.Time

	// lock to protect the set of committed indexes
	// shared lock will be acquired when writing new content to allow it to happen in parallel
	// exclusive lock will be acquired during compaction or refresh.
	indexesLock            sync.RWMutex
	permissiveCacheLoading bool

	// maybeRefreshIndexes() will call Refresh() after this point in ime.
	// +checklocks:indexesLock
	refreshIndexesAfter time.Time

	format format.Provider

	checkInvariantsOnUnlock bool
	minPreambleLength       int
	maxPreambleLength       int
	paddingUnit             int

	// logger where logs should be written
	log logging.Logger

	// logger associated with the context that opened the repository.
	contextLogger  logging.Logger
	repoLogManager *repodiag.LogManager
	internalLogger *zap.SugaredLogger // backing logger for 'sharedBaseLogger'

	metricsStruct
}

// IsReadOnly returns whether this instance of the SharedManager only supports
// reads or if it also supports mutations to the index.
func (sm *SharedManager) IsReadOnly() bool {
	return sm.st.IsReadOnly()
}

// LoadIndexBlob return index information loaded from the specified blob.
func (sm *SharedManager) LoadIndexBlob(ctx context.Context, ibid blob.ID, d *gather.WriteBuffer) ([]Info, error) {
	err := sm.st.GetBlob(ctx, ibid, 0, -1, d)
	if err != nil {
		return nil, errors.Wrapf(err, "could not find index blob %q", ibid)
	}

	return ParseIndexBlob(ibid, d.Bytes(), sm.format)
}

// IndexReaderV0 return an index reader for reading V0 indexes.
func (sm *SharedManager) IndexReaderV0() IndexBlobReader {
	return sm.indexBlobManagerV0
}

// IndexReaderV1 return an index reader for reading V1 indexes.
func (sm *SharedManager) IndexReaderV1() IndexBlobReader {
	return sm.indexBlobManagerV1
}

func (sm *SharedManager) readPackFileLocalIndex(ctx context.Context, packFile blob.ID, packFileLength int64, output *gather.WriteBuffer) error {
	var err error

	if packFileLength >= indexRecoverPostambleSize {
		if err = sm.attemptReadPackFileLocalIndex(ctx, packFile, packFileLength-indexRecoverPostambleSize, indexRecoverPostambleSize, output); err == nil {
			sm.log.Debugf("recovered %v index bytes from blob %v using optimized method", output.Length(), packFile)
			return nil
		}

		sm.log.Debugf("unable to recover using optimized method: %v", err)
	}

	if err = sm.attemptReadPackFileLocalIndex(ctx, packFile, 0, -1, output); err == nil {
		sm.log.Debugf("recovered %v index bytes from blob %v using full blob read", output.Length(), packFile)

		return nil
	}

	return err
}

func (sm *SharedManager) attemptReadPackFileLocalIndex(ctx context.Context, packFile blob.ID, offset, length int64, output *gather.WriteBuffer) error {
	var payload gather.WriteBuffer
	defer payload.Close()

	output.Reset()

	err := sm.st.GetBlob(ctx, packFile, offset, length, &payload)
	if err != nil {
		return errors.Wrapf(err, "error getting blob %v", packFile)
	}

	postamble := findPostamble(payload.Bytes().ToByteSlice())
	if postamble == nil {
		return errors.Errorf("unable to find valid postamble in file %v", packFile)
	}

	if uint32(offset) > postamble.localIndexOffset { //nolint:gosec
		return errors.Errorf("not enough data read during optimized attempt %v", packFile)
	}

	postamble.localIndexOffset -= uint32(offset) //nolint:gosec

	//nolint:gosec
	if uint64(postamble.localIndexOffset+postamble.localIndexLength) > uint64(payload.Length()) {
		// invalid offset/length
		return errors.Errorf("unable to find valid local index in file %v - invalid offset/length", packFile)
	}

	var encryptedLocalIndexBytes gather.WriteBuffer
	defer encryptedLocalIndexBytes.Close()

	if err := payload.AppendSectionTo(&encryptedLocalIndexBytes, int(postamble.localIndexOffset), int(postamble.localIndexLength)); err != nil {
		// should never happen
		return errors.Wrap(err, "error appending to local index bytes")
	}

	return errors.Wrap(
		sm.decryptAndVerify(encryptedLocalIndexBytes.Bytes(), postamble.localIndexIV, output),
		"unable to decrypt local index")
}

// +checklocks:sm.indexesLock
func (sm *SharedManager) loadPackIndexesLocked(ctx context.Context) error {
	nextSleepTime := 100 * time.Millisecond //nolint:mnd

	for i := range indexLoadAttempts {
		ibm, err0 := sm.indexBlobManager(ctx)
		if err0 != nil {
			return err0
		}

		if err := ctx.Err(); err != nil {
			//nolint:wrapcheck
			return err
		}

		if i > 0 {
			// invalidate any list caches.
			if err := sm.st.FlushCaches(ctx); err != nil {
				sm.log.Errorw("unable to flush caches", "err", err)
			}

			sm.log.Debugf("encountered NOT_FOUND when loading, sleeping %v before retrying #%v", nextSleepTime, i)
			time.Sleep(nextSleepTime)
			nextSleepTime *= 2
		}

		indexBlobs, ignoreDeletedBefore, err := ibm.ListActiveIndexBlobs(ctx)
		if err != nil {
			return errors.Wrap(err, "error listing index blobs")
		}

		var indexBlobIDs []blob.ID
		for _, b := range indexBlobs {
			indexBlobIDs = append(indexBlobIDs, b.BlobID)
		}

		err = sm.committedContents.fetchIndexBlobs(ctx, sm.permissiveCacheLoading, indexBlobIDs)
		if err == nil {
			err = sm.committedContents.use(ctx, indexBlobIDs, ignoreDeletedBefore)
			if err != nil {
				return err
			}

			if len(indexBlobs) > indexBlobCompactionWarningThreshold {
				sm.log.Errorf("Found too many index blobs (%v), this may result in degraded performance.\n\nPlease ensure periodic repository maintenance is enabled or run 'kopia maintenance'.", len(indexBlobs))
			}

			sm.refreshIndexesAfter = sm.timeNow().Add(indexRefreshFrequency)

			return nil
		}

		if !errors.Is(err, blob.ErrBlobNotFound) {
			return err
		}
	}

	return errors.Errorf("unable to load pack indexes despite %v retries", indexLoadAttempts)
}

func (sm *SharedManager) getCacheForContentID(id ID) cache.ContentCache {
	if id.HasPrefix() {
		return sm.metadataCache
	}

	return sm.contentCache
}

// indexBlobManager return the index manager for content.
func (sm *SharedManager) indexBlobManager(ctx context.Context) (indexblob.Manager, error) {
	mp, mperr := sm.format.GetMutableParameters(ctx)
	if mperr != nil {
		return nil, errors.Wrap(mperr, "mutable parameters")
	}

	var q indexblob.Manager = sm.indexBlobManagerV0
	if mp.EpochParameters.Enabled {
		q = sm.indexBlobManagerV1
	}

	return q, nil
}

func (sm *SharedManager) decryptContentAndVerify(payload gather.Bytes, bi Info, output *gather.WriteBuffer) error {
	sm.Stats.readContent(payload.Length())

	var hashBuf [hashing.MaxHashSize]byte

	iv := getPackedContentIV(hashBuf[:0], bi.ContentID)

	// reserved for future use
	if k := bi.EncryptionKeyID; k != 0 {
		return errors.Errorf("unsupported encryption key ID: %v", k)
	}

	h := bi.CompressionHeaderID
	if h == 0 {
		return errors.Wrapf(
			sm.decryptAndVerify(payload, iv, output),
			"invalid checksum at %v offset %v length %v/%v", bi.PackBlobID, bi.PackOffset, bi.PackedLength, payload.Length())
	}

	var tmp gather.WriteBuffer
	defer tmp.Close()

	if err := sm.decryptAndVerify(payload, iv, &tmp); err != nil {
		return errors.Wrapf(err, "invalid checksum at %v offset %v length %v/%v", bi.PackBlobID, bi.PackOffset, bi.PackedLength, payload.Length())
	}

	c := compression.ByHeaderID[h]
	if c == nil {
		return errors.Errorf("unsupported compressor %x", h)
	}

	t0 := timetrack.StartTimer()

	if err := c.Decompress(output, tmp.Bytes().Reader(), true); err != nil {
		return errors.Wrap(err, "error decompressing")
	}

	sm.decompressedBytes.Observe(int64(tmp.Length()), t0.Elapsed())

	return nil
}

func (sm *SharedManager) decryptAndVerify(encrypted gather.Bytes, iv []byte, output *gather.WriteBuffer) error {
	t0 := timetrack.StartTimer()

	if err := sm.format.Encryptor().Decrypt(encrypted, iv, output); err != nil {
		sm.Stats.foundInvalidContent()
		return errors.Wrap(err, "decrypt")
	}

	sm.decryptedBytes.Observe(int64(encrypted.Length()), t0.Elapsed())
	sm.Stats.foundValidContent()
	sm.Stats.decrypted(output.Length())

	// already verified
	return nil
}

// IndexBlobs returns the list of active index blobs.
func (sm *SharedManager) IndexBlobs(ctx context.Context, includeInactive bool) ([]indexblob.Metadata, error) {
	if includeInactive {
		var result []indexblob.Metadata

		for _, prefix := range allIndexBlobPrefixes {
			blobs, err := blob.ListAllBlobs(ctx, sm.st, prefix)
			if err != nil {
				return nil, errors.Wrapf(err, "error listing %v blogs", prefix)
			}

			for _, bm := range blobs {
				result = append(result, indexblob.Metadata{Metadata: bm})
			}
		}

		return result, nil
	}

	ibm, err0 := sm.indexBlobManager(ctx)
	if err0 != nil {
		return nil, err0
	}

	blobs, _, err := ibm.ListActiveIndexBlobs(ctx)

	//nolint:wrapcheck
	return blobs, err
}

func newOwnWritesCache(ctx context.Context, st blob.Storage, caching *CachingOptions) (blob.Storage, error) {
	cacheSt, err := newCacheBackingStorage(ctx, caching, "own-writes")
	if err != nil {
		return nil, errors.Wrap(err, "unable to get list cache backing storage")
	}

	return ownwrites.NewWrapper(st, cacheSt, cachedIndexBlobPrefixes, ownWritesCacheDuration), nil
}

func newListCache(ctx context.Context, st blob.Storage, caching *CachingOptions) (blob.Storage, error) {
	cacheSt, err := newCacheBackingStorage(ctx, caching, "blob-list")
	if err != nil {
		return nil, errors.Wrap(err, "unable to get list cache backing storage")
	}

	return listcache.NewWrapper(st, cacheSt, cachedIndexBlobPrefixes, caching.HMACSecret, caching.MaxListCacheDuration.DurationOrDefault(0)), nil
}

func newCacheBackingStorage(ctx context.Context, caching *CachingOptions, subdir string) (blob.Storage, error) {
	if caching.CacheDirectory == "" {
		return nil, nil
	}

	blobListCacheDir := filepath.Join(caching.CacheDirectory, subdir)

	if _, err := os.Stat(blobListCacheDir); os.IsNotExist(err) {
		if err := os.MkdirAll(blobListCacheDir, cache.DirMode); err != nil {
			return nil, errors.Wrap(err, "error creating list cache directory")
		}
	}

	//nolint:wrapcheck
	return filesystem.New(ctx, &filesystem.Options{
		Path: blobListCacheDir,
		Options: sharded.Options{
			DirectoryShards: []int{},
		},
	}, false)
}

func (sm *SharedManager) namedLogger(n string) logging.Logger {
	if sm.internalLogger != nil {
		return logging.Broadcast(
			sm.contextLogger,
			sm.internalLogger.Named("["+n+"]"))
	}

	return sm.contextLogger
}

func contentCacheSweepSettings(caching *CachingOptions) cache.SweepSettings {
	return cache.SweepSettings{
		MaxSizeBytes: caching.ContentCacheSizeBytes,
		LimitBytes:   caching.ContentCacheSizeLimitBytes,
		MinSweepAge:  caching.MinContentSweepAge.DurationOrDefault(DefaultDataCacheSweepAge),
	}
}

func metadataCacheSizeSweepSettings(caching *CachingOptions) cache.SweepSettings {
	return cache.SweepSettings{
		MaxSizeBytes: caching.EffectiveMetadataCacheSizeBytes(),
		LimitBytes:   caching.MetadataCacheSizeLimitBytes,
		MinSweepAge:  caching.MinMetadataSweepAge.DurationOrDefault(DefaultMetadataCacheSweepAge),
	}
}

func indexBlobCacheSweepSettings(caching *CachingOptions) cache.SweepSettings {
	return cache.SweepSettings{
		MaxSizeBytes: caching.EffectiveMetadataCacheSizeBytes(),
		MinSweepAge:  caching.MinMetadataSweepAge.DurationOrDefault(DefaultMetadataCacheSweepAge),
	}
}

func (sm *SharedManager) setupCachesAndIndexManagers(ctx context.Context, caching *CachingOptions, mr *metrics.Registry) error {
	dataCache, err := cache.NewContentCache(ctx, sm.st, cache.Options{
		BaseCacheDirectory: caching.CacheDirectory,
		CacheSubDir:        "contents",
		HMACSecret:         caching.HMACSecret,
		Sweep:              contentCacheSweepSettings(caching),
	}, mr)
	if err != nil {
		return errors.Wrap(err, "unable to initialize content cache")
	}

	metadataCache, err := cache.NewContentCache(ctx, sm.st, cache.Options{
		BaseCacheDirectory: caching.CacheDirectory,
		CacheSubDir:        "metadata",
		HMACSecret:         caching.HMACSecret,
		FetchFullBlobs:     true,
		Sweep:              metadataCacheSizeSweepSettings(caching),
	}, mr)
	if err != nil {
		return errors.Wrap(err, "unable to initialize metadata cache")
	}

	indexBlobStorage, err := cache.NewStorageOrNil(ctx, caching.CacheDirectory, caching.EffectiveMetadataCacheSizeBytes(), "index-blobs")
	if err != nil {
		return errors.Wrap(err, "unable to initialize index blob cache storage")
	}

	indexBlobCache, err := cache.NewPersistentCache(ctx, "index-blobs",
		indexBlobStorage,
		cacheprot.ChecksumProtection(caching.HMACSecret),
		indexBlobCacheSweepSettings(caching),
		mr, sm.timeNow)
	if err != nil {
		return errors.Wrap(err, "unable to create index blob cache")
	}

	ownWritesCachingSt, err := newOwnWritesCache(ctx, sm.st, caching)
	if err != nil {
		return errors.Wrap(err, "unable to initialize own writes cache")
	}

	cachedSt, err := newListCache(ctx, ownWritesCachingSt, caching)
	if err != nil {
		return errors.Wrap(err, "unable to initialize list cache")
	}

	enc := indexblob.NewEncryptionManager(
		cachedSt,
		sm.format,
		indexBlobCache,
		sm.namedLogger("encrypted-blob-manager"))

	// set up legacy index blob manager
	sm.indexBlobManagerV0 = indexblob.NewManagerV0(
		cachedSt,
		enc,
		sm.timeNow,
		sm.format,
		sm.namedLogger("index-blob-manager"),
	)

	// set up new index blob manager
	sm.indexBlobManagerV1 = indexblob.NewManagerV1(
		cachedSt,
		enc,
		epoch.NewManager(cachedSt,
			epochParameters{sm.format},
			func(ctx context.Context, blobIDs []blob.ID, outputPrefix blob.ID) error {
				return errors.Wrap(sm.indexBlobManagerV1.CompactEpoch(ctx, blobIDs, outputPrefix), "CompactEpoch")
			},
			sm.namedLogger("epoch-manager"),
			sm.timeNow),
		sm.timeNow,
		sm.format,
		sm.namedLogger("index-blob-manager"),
	)

	// once everything is ready, set it up
	sm.contentCache = dataCache
	sm.metadataCache = metadataCache
	sm.indexBlobCache = indexBlobCache
	sm.committedContents = newCommittedContentIndex(caching,
		sm.format.Encryptor().Overhead,
		sm.format,
		sm.permissiveCacheLoading,
		enc.GetEncryptedBlob,
		sm.namedLogger("committed-content-index"),
		caching.MinIndexSweepAge.DurationOrDefault(DefaultIndexCacheSweepAge))

	return nil
}

type epochParameters struct {
	prov format.Provider
}

func (p epochParameters) GetParameters(ctx context.Context) (*epoch.Parameters, error) {
	mp, mperr := p.prov.GetMutableParameters(ctx)
	if mperr != nil {
		return nil, errors.Wrap(mperr, "mutable parameters")
	}

	return &mp.EpochParameters, nil
}

// EpochManager returns the epoch manager.
func (sm *SharedManager) EpochManager(ctx context.Context) (*epoch.Manager, bool, error) {
	ibm, err := sm.indexBlobManager(ctx)
	if err != nil {
		return nil, false, err
	}

	ibm1, ok := ibm.(*indexblob.ManagerV1)
	if !ok {
		return nil, false, nil
	}

	return ibm1.EpochManager(), true, nil
}

// CloseShared releases all resources in a shared manager.
func (sm *SharedManager) CloseShared(ctx context.Context) error {
	if err := sm.committedContents.close(); err != nil {
		return errors.Wrap(err, "error closing committed content index")
	}

	sm.contentCache.Close(ctx)
	sm.metadataCache.Close(ctx)
	sm.indexBlobCache.Close(ctx)

	if sm.internalLogger != nil {
		sm.internalLogger.Sync() //nolint:errcheck
	}

	sm.indexBlobManagerV1.EpochManager().Flush()

	return nil
}

// AlsoLogToContentLog wraps the provided content so that all logs are also sent to
// internal content log.
func (sm *SharedManager) AlsoLogToContentLog(ctx context.Context) context.Context {
	sm.repoLogManager.Enable()

	return logging.WithAdditionalLogger(ctx, func(_ string) logging.Logger {
		return sm.log
	})
}

func (sm *SharedManager) shouldRefreshIndexes() bool {
	sm.indexesLock.RLock()
	defer sm.indexesLock.RUnlock()

	return sm.timeNow().After(sm.refreshIndexesAfter)
}

// PrepareUpgradeToIndexBlobManagerV1 prepares the repository for migrating to IndexBlobManagerV1.
func (sm *SharedManager) PrepareUpgradeToIndexBlobManagerV1(ctx context.Context) error {
	//nolint:wrapcheck
	return sm.indexBlobManagerV1.PrepareUpgradeToIndexBlobManagerV1(ctx, sm.indexBlobManagerV0)
}

// NewSharedManager returns SharedManager that is used by SessionWriteManagers on top of a repository.
func NewSharedManager(ctx context.Context, st blob.Storage, prov format.Provider, caching *CachingOptions, opts *ManagerOptions, repoLogManager *repodiag.LogManager, mr *metrics.Registry) (*SharedManager, error) {
	opts = opts.CloneOrDefault()
	if opts.TimeNow == nil {
		opts.TimeNow = clock.Now
	}

	sm := &SharedManager{
		st:                      st,
		Stats:                   new(Stats),
		timeNow:                 opts.TimeNow,
		format:                  prov,
		permissiveCacheLoading:  opts.PermissiveCacheLoading,
		minPreambleLength:       defaultMinPreambleLength,
		maxPreambleLength:       defaultMaxPreambleLength,
		paddingUnit:             defaultPaddingUnit,
		checkInvariantsOnUnlock: os.Getenv("KOPIA_VERIFY_INVARIANTS") != "",
		repoLogManager:          repoLogManager,
		contextLogger:           logging.Module(FormatLogModule)(ctx),

		metricsStruct: initMetricsStruct(mr),
	}

	if !opts.DisableInternalLog {
		sm.internalLogger = sm.repoLogManager.NewLogger()
	}

	sm.log = sm.namedLogger("shared-manager")

	caching = caching.CloneOrDefault()

	if err := sm.setupCachesAndIndexManagers(ctx, caching, mr); err != nil {
		return nil, errors.Wrap(err, "error setting up read manager caches")
	}

	sm.indexesLock.Lock()
	defer sm.indexesLock.Unlock()

	if err := sm.loadPackIndexesLocked(ctx); err != nil {
		return nil, errors.Wrap(err, "error loading indexes")
	}

	return sm, nil
}
