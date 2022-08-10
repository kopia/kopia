package content

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/listcache"
	"github.com/kopia/kopia/internal/ownwrites"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/blob/sharded"
	"github.com/kopia/kopia/repo/compression"
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
	LegacyIndexBlobPrefix,
	compactionLogBlobPrefix,
	cleanupBlobPrefix,

	epoch.UncompactedIndexBlobPrefix,
	epoch.EpochMarkerIndexBlobPrefix,
	epoch.SingleEpochCompactionBlobPrefix,
	epoch.RangeCheckpointIndexBlobPrefix,
}

//nolint:gochecknoglobals
var allIndexBlobPrefixes = []blob.ID{
	LegacyIndexBlobPrefix,
	epoch.UncompactedIndexBlobPrefix,
	epoch.SingleEpochCompactionBlobPrefix,
	epoch.RangeCheckpointIndexBlobPrefix,
}

// indexBlobManager is the API of index blob manager as used by content manager.
type indexBlobManager interface {
	writeIndexBlobs(ctx context.Context, data []gather.Bytes, sessionID SessionID) ([]blob.Metadata, error)
	listActiveIndexBlobs(ctx context.Context) ([]IndexBlobInfo, time.Time, error)
	compact(ctx context.Context, opts CompactOptions) error
	flushCache(ctx context.Context)
	invalidate(ctx context.Context)
}

// SharedManager is responsible for read-only access to committed data.
type SharedManager struct {
	// +checkatomic
	refCount int32 // number of Manager objects that refer to this SharedManager
	// +checkatomic
	closed int32 // set to 1 if shared manager has been closed

	Stats *Stats
	st    blob.Storage

	indexBlobManagerV0 *indexBlobManagerV0
	indexBlobManagerV1 *indexBlobManagerV1

	contentCache      cache.ContentCache
	metadataCache     cache.ContentCache
	indexBlobCache    *cache.PersistentCache
	committedContents *committedContentIndex
	transf            *transformedBlobMgr
	timeNow           func() time.Time

	// lock to protect the set of commtited indexes
	// shared lock will be acquired when writing new content to allow it to happen in parallel
	// exclusive lock will be acquired during compaction or refresh.
	indexesLock sync.RWMutex

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
	contextLogger      logging.Logger
	internalLogManager *internalLogManager
	internalLogger     *zap.SugaredLogger // backing logger for 'sharedBaseLogger'
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

	if uint32(offset) > postamble.localIndexOffset {
		return errors.Errorf("not enough data read during optimized attempt %v", packFile)
	}

	postamble.localIndexOffset -= uint32(offset)

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
	nextSleepTime := 100 * time.Millisecond //nolint:gomnd

	for i := 0; i < indexLoadAttempts; i++ {
		ibm, err0 := sm.indexBlobManager()
		if err0 != nil {
			return err0
		}

		if err := ctx.Err(); err != nil {
			//nolint:wrapcheck
			return err
		}

		if i > 0 {
			ibm.flushCache(ctx)
			sm.log.Debugf("encountered NOT_FOUND when loading, sleeping %v before retrying #%v", nextSleepTime, i)
			time.Sleep(nextSleepTime)
			nextSleepTime *= 2
		}

		indexBlobs, ignoreDeletedBefore, err := ibm.listActiveIndexBlobs(ctx)
		if err != nil {
			return errors.Wrap(err, "error listing index blobs")
		}

		var indexBlobIDs []blob.ID
		for _, b := range indexBlobs {
			indexBlobIDs = append(indexBlobIDs, b.BlobID)
		}

		err = sm.committedContents.fetchIndexBlobs(ctx, indexBlobIDs)
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

func (sm *SharedManager) indexBlobManager() (indexBlobManager, error) {
	mp, mperr := sm.format.GetMutableParameters()
	if mperr != nil {
		return nil, errors.Wrap(mperr, "mutable parameters")
	}

	if mp.EpochParameters.Enabled {
		return sm.indexBlobManagerV1, nil
	}

	return sm.indexBlobManagerV0, nil
}

func (sm *SharedManager) decryptContentAndVerify(payload gather.Bytes, bi Info, output *gather.WriteBuffer) error {
	sm.Stats.readContent(payload.Length())

	var hashBuf [hashing.MaxHashSize]byte

	iv := getPackedContentIV(hashBuf[:0], bi.GetContentID())

	// reserved for future use
	if k := bi.GetEncryptionKeyID(); k != 0 {
		return errors.Errorf("unsupported encryption key ID: %v", k)
	}

	h := bi.GetCompressionHeaderID()
	if h == 0 {
		return errors.Wrapf(
			sm.decryptAndVerify(payload, iv, output),
			"invalid checksum at %v offset %v length %v/%v", bi.GetPackBlobID(), bi.GetPackOffset(), bi.GetPackedLength(), payload.Length())
	}

	var tmp gather.WriteBuffer
	defer tmp.Close()

	if err := sm.decryptAndVerify(payload, iv, &tmp); err != nil {
		return errors.Wrapf(err, "invalid checksum at %v offset %v length %v/%v", bi.GetPackBlobID(), bi.GetPackOffset(), bi.GetPackedLength(), payload.Length())
	}

	c := compression.ByHeaderID[h]
	if c == nil {
		return errors.Errorf("unsupported compressor %x", h)
	}

	if err := c.Decompress(output, tmp.Bytes().Reader(), true); err != nil {
		return errors.Wrap(err, "error decompressing")
	}

	return nil
}

func (sm *SharedManager) decryptAndVerify(encrypted gather.Bytes, iv []byte, output *gather.WriteBuffer) error {
	if err := sm.format.Transformer().FromRepository(encrypted, iv, output); err != nil {
		sm.Stats.foundInvalidContent()
		return errors.Wrap(err, "decrypt")
	}

	sm.Stats.foundValidContent()
	sm.Stats.decrypted(output.Length())

	// already verified
	return nil
}

// IndexBlobs returns the list of active index blobs.
func (sm *SharedManager) IndexBlobs(ctx context.Context, includeInactive bool) ([]IndexBlobInfo, error) {
	if includeInactive {
		var result []IndexBlobInfo

		for _, prefix := range allIndexBlobPrefixes {
			blobs, err := blob.ListAllBlobs(ctx, sm.st, prefix)
			if err != nil {
				return nil, errors.Wrapf(err, "error listing %v blogs", prefix)
			}

			for _, bm := range blobs {
				result = append(result, IndexBlobInfo{Metadata: bm})
			}
		}

		return result, nil
	}

	ibm, err0 := sm.indexBlobManager()
	if err0 != nil {
		return nil, err0
	}

	blobs, _, err := ibm.listActiveIndexBlobs(ctx)

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

func (sm *SharedManager) setupReadManagerCaches(ctx context.Context, caching *CachingOptions) error {
	dataCache, err := cache.NewContentCache(ctx, sm.st, cache.Options{
		BaseCacheDirectory: caching.CacheDirectory,
		CacheSubDir:        "contents",
		HMACSecret:         caching.HMACSecret,
		Sweep: cache.SweepSettings{
			MaxSizeBytes: caching.MaxCacheSizeBytes,
			MinSweepAge:  caching.MinContentSweepAge.DurationOrDefault(DefaultDataCacheSweepAge),
		},
	})
	if err != nil {
		return errors.Wrap(err, "unable to initialize content cache")
	}

	metadataCacheSize := caching.MaxMetadataCacheSizeBytes
	if metadataCacheSize == 0 && caching.MaxCacheSizeBytes > 0 {
		metadataCacheSize = caching.MaxCacheSizeBytes
	}

	metadataCache, err := cache.NewContentCache(ctx, sm.st, cache.Options{
		BaseCacheDirectory: caching.CacheDirectory,
		CacheSubDir:        "metadata",
		HMACSecret:         caching.HMACSecret,
		FetchFullBlobs:     true,
		Sweep: cache.SweepSettings{
			MaxSizeBytes: metadataCacheSize,
			MinSweepAge:  caching.MinMetadataSweepAge.DurationOrDefault(DefaultMetadataCacheSweepAge),
		},
	})
	if err != nil {
		return errors.Wrap(err, "unable to initialize metadata cache")
	}

	indexBlobStorage, err := cache.NewStorageOrNil(ctx, caching.CacheDirectory, metadataCacheSize, "index-blobs")
	if err != nil {
		return errors.Wrap(err, "unable to initialize index blob cache storage")
	}

	indexBlobCache, err := cache.NewPersistentCache(ctx, "index blob cache", indexBlobStorage, cache.ChecksumProtection(caching.HMACSecret), cache.SweepSettings{
		MaxSizeBytes: metadataCacheSize,
		MinSweepAge:  caching.MinMetadataSweepAge.DurationOrDefault(DefaultMetadataCacheSweepAge),
	})
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

	sm.transf = &transformedBlobMgr{
		st:             cachedSt,
		t:              sm.format,
		indexBlobCache: indexBlobCache,
		log:            sm.namedLogger("transformed-blob-manager"),
	}

	// set up legacy index blob manager
	sm.indexBlobManagerV0 = &indexBlobManagerV0{
		st:                cachedSt,
		transf:            sm.transf,
		timeNow:           sm.timeNow,
		formattingOptions: sm.format,
		log:               sm.namedLogger("index-blob-manager"),
	}

	// set up new index blob manager
	sm.indexBlobManagerV1 = &indexBlobManagerV1{
		st:                cachedSt,
		transf:            sm.transf,
		timeNow:           sm.timeNow,
		formattingOptions: sm.format,
		log:               sm.namedLogger("index-blob-manager"),
	}

	sm.indexBlobManagerV1.epochMgr = epoch.NewManager(cachedSt, epochParameters{sm.format}, sm.indexBlobManagerV1.compactEpoch, sm.namedLogger("epoch-manager"), sm.timeNow)

	// once everything is ready, set it up
	sm.contentCache = dataCache
	sm.metadataCache = metadataCache
	sm.indexBlobCache = indexBlobCache
	sm.committedContents = newCommittedContentIndex(caching,
		sm.format.Transformer().Overhead,
		sm.format,
		sm.transf.readBlob,
		sm.namedLogger("committed-content-index"),
		caching.MinIndexSweepAge.DurationOrDefault(DefaultIndexCacheSweepAge))

	return nil
}

type epochParameters struct {
	prov format.Provider
}

func (p epochParameters) GetParameters() (*epoch.Parameters, error) {
	mp, mperr := p.prov.GetMutableParameters()
	if mperr != nil {
		return nil, errors.Wrap(mperr, "mutable parameters")
	}

	return &mp.EpochParameters, nil
}

// EpochManager returns the epoch manager.
func (sm *SharedManager) EpochManager() (*epoch.Manager, bool, error) {
	ibm, err := sm.indexBlobManager()
	if err != nil {
		return nil, false, err
	}

	ibm1, ok := ibm.(*indexBlobManagerV1)
	if !ok {
		return nil, false, nil
	}

	return ibm1.epochMgr, true, nil
}

// AddRef adds a reference to shared manager to prevents its closing on Release().
func (sm *SharedManager) addRef() {
	if atomic.LoadInt32(&sm.closed) != 0 {
		panic("attempted to re-use closed SharedManager")
	}

	atomic.AddInt32(&sm.refCount, 1)
}

// release removes a reference to the shared manager and destroys it if no more references are remaining.
func (sm *SharedManager) release(ctx context.Context) error {
	if atomic.LoadInt32(&sm.closed) != 0 {
		// already closed
		return nil
	}

	remaining := atomic.AddInt32(&sm.refCount, -1)
	if remaining != 0 {
		sm.log.Debugf("not closing shared manager, remaining = %v", remaining)

		return nil
	}

	atomic.StoreInt32(&sm.closed, 1)

	sm.log.Debugf("closing shared manager")

	if err := sm.committedContents.close(); err != nil {
		return errors.Wrap(err, "error closing committed content index")
	}

	sm.contentCache.Close(ctx)
	sm.metadataCache.Close(ctx)
	sm.indexBlobCache.Close(ctx)

	if sm.internalLogger != nil {
		sm.internalLogger.Sync() //nolint:errcheck
	}

	sm.internalLogManager.Close(ctx)

	sm.indexBlobManagerV1.epochMgr.Flush()

	return errors.Wrap(sm.st.Close(ctx), "error closing storage")
}

// AlsoLogToContentLog wraps the provided content so that all logs are also sent to
// internal content log.
func (sm *SharedManager) AlsoLogToContentLog(ctx context.Context) context.Context {
	sm.internalLogManager.enable()

	return logging.AlsoLogTo(ctx, sm.log)
}

func (sm *SharedManager) shouldRefreshIndexes() bool {
	sm.indexesLock.RLock()
	defer sm.indexesLock.RUnlock()

	return sm.timeNow().After(sm.refreshIndexesAfter)
}

// NewSharedManager returns SharedManager that is used by SessionWriteManagers on top of a repository.
func NewSharedManager(ctx context.Context, st blob.Storage, prov format.Provider, caching *CachingOptions, opts *ManagerOptions) (*SharedManager, error) {
	opts = opts.CloneOrDefault()
	if opts.TimeNow == nil {
		opts.TimeNow = clock.Now
	}

	// create internal logger that will be writing logs as encrypted repository blobs.
	ilm := newInternalLogManager(ctx, st, prov)

	// sharedBaseLogger writes to the both context and internal log
	// and is used as a base for all content manager components.
	var internalLog *zap.SugaredLogger

	// capture logger (usually console or log file) associated with current context.
	if !opts.DisableInternalLog {
		internalLog = ilm.NewLogger()
	}

	sm := &SharedManager{
		st:                      st,
		Stats:                   new(Stats),
		timeNow:                 opts.TimeNow,
		format:                  prov,
		minPreambleLength:       defaultMinPreambleLength,
		maxPreambleLength:       defaultMaxPreambleLength,
		paddingUnit:             defaultPaddingUnit,
		checkInvariantsOnUnlock: os.Getenv("KOPIA_VERIFY_INVARIANTS") != "",
		internalLogManager:      ilm,
		internalLogger:          internalLog,
		contextLogger:           logging.Module(FormatLogModule)(ctx),
	}

	// remember logger defined for the context.
	sm.log = sm.namedLogger("shared-manager")

	caching = caching.CloneOrDefault()

	if err := sm.setupReadManagerCaches(ctx, caching); err != nil {
		return nil, errors.Wrap(err, "error setting up read manager caches")
	}

	sm.indexesLock.Lock()
	defer sm.indexesLock.Unlock()

	if err := sm.loadPackIndexesLocked(ctx); err != nil {
		return nil, errors.Wrap(err, "error loading indexes")
	}

	return sm, nil
}
