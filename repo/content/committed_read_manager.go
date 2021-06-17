package content

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/buf"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/listcache"
	"github.com/kopia/kopia/internal/ownwrites"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/logging"
)

// number of bytes to read from each pack index when recovering the index.
// per-pack indexes are usually short (<100-200 contents).
const indexRecoverPostambleSize = 8192

const ownWritesCacheDuration = 15 * time.Minute

var cachedIndexBlobPrefixes = []blob.ID{IndexBlobPrefix, compactionLogBlobPrefix, cleanupBlobPrefix}

// indexBlobManager is the API of index blob manager as used by content manager.
type indexBlobManager interface {
	writeIndexBlob(ctx context.Context, data []byte, sessionID SessionID) (blob.Metadata, error)
	listActiveIndexBlobs(ctx context.Context) ([]IndexBlobInfo, error)
	compact(ctx context.Context, opts CompactOptions) error
	flushCache(ctx context.Context)
}

// SharedManager is responsible for read-only access to committed data.
type SharedManager struct {
	refCount int32 // number of Manager objects that refer to this SharedManager
	closed   int32 // set to 1 if shared manager has been closed

	Stats             *Stats
	st                blob.Storage
	indexBlobManager  indexBlobManager
	contentCache      contentCache
	metadataCache     contentCache
	committedContents *committedContentIndex
	crypter           *Crypter
	enc               *encryptedBlobMgr
	timeNow           func() time.Time

	format                  FormattingOptions
	checkInvariantsOnUnlock bool
	writeFormatVersion      int32 // format version to write
	maxPackSize             int
	minPreambleLength       int
	maxPreambleLength       int
	paddingUnit             int
	repositoryFormatBytes   []byte
	indexVersion            int
	encryptionBufferPool    *buf.Pool

	// logger where logs should be written
	log logging.Logger

	// base logger used by other related components with their own prefixes,
	// do not log there directly.
	sharedBaseLogger   logging.Logger
	internalLogManager *internalLogManager
	internalLogger     *internalLogger // backing logger for 'sharedBaseLogger'
}

// Crypter returns the crypter.
func (sm *SharedManager) Crypter() *Crypter {
	return sm.crypter
}

func (sm *SharedManager) readPackFileLocalIndex(ctx context.Context, packFile blob.ID, packFileLength int64) ([]byte, error) {
	if packFileLength >= indexRecoverPostambleSize {
		data, err := sm.attemptReadPackFileLocalIndex(ctx, packFile, packFileLength-indexRecoverPostambleSize, indexRecoverPostambleSize)
		if err == nil {
			sm.log.Debugf("recovered %v index bytes from blob %v using optimized method", len(data), packFile)
			return data, nil
		}

		sm.log.Debugf("unable to recover using optimized method: %v", err)
	}

	data, err := sm.attemptReadPackFileLocalIndex(ctx, packFile, 0, -1)
	if err == nil {
		sm.log.Debugf("recovered %v index bytes from blob %v using full blob read", len(data), packFile)
		return data, nil
	}

	return nil, err
}

func (sm *SharedManager) attemptReadPackFileLocalIndex(ctx context.Context, packFile blob.ID, offset, length int64) ([]byte, error) {
	payload, err := sm.st.GetBlob(ctx, packFile, offset, length)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting blob %v", packFile)
	}

	postamble := findPostamble(payload)
	if postamble == nil {
		return nil, errors.Errorf("unable to find valid postamble in file %v", packFile)
	}

	if uint32(offset) > postamble.localIndexOffset {
		return nil, errors.Errorf("not enough data read during optimized attempt %v", packFile)
	}

	postamble.localIndexOffset -= uint32(offset)

	if uint64(postamble.localIndexOffset+postamble.localIndexLength) > uint64(len(payload)) {
		// invalid offset/length
		return nil, errors.Errorf("unable to find valid local index in file %v - invalid offset/length", packFile)
	}

	encryptedLocalIndexBytes := payload[postamble.localIndexOffset : postamble.localIndexOffset+postamble.localIndexLength]
	if encryptedLocalIndexBytes == nil {
		return nil, errors.Errorf("unable to find valid local index in file %v", packFile)
	}

	localIndexBytes, err := sm.decryptAndVerify(encryptedLocalIndexBytes, postamble.localIndexIV)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decrypt local index")
	}

	return localIndexBytes, nil
}

func (sm *SharedManager) loadPackIndexesUnlocked(ctx context.Context) error {
	nextSleepTime := 100 * time.Millisecond //nolint:gomnd

	for i := 0; i < indexLoadAttempts; i++ {
		if err := ctx.Err(); err != nil {
			// nolint:wrapcheck
			return err
		}

		if i > 0 {
			sm.indexBlobManager.flushCache(ctx)
			sm.log.Debugf("encountered NOT_FOUND when loading, sleeping %v before retrying #%v", nextSleepTime, i)
			time.Sleep(nextSleepTime)
			nextSleepTime *= 2
		}

		indexBlobs, err := sm.indexBlobManager.listActiveIndexBlobs(ctx)
		if err != nil {
			return errors.Wrap(err, "error listing index blobs")
		}

		var indexBlobIDs []blob.ID
		for _, b := range indexBlobs {
			indexBlobIDs = append(indexBlobIDs, b.BlobID)
		}

		err = sm.committedContents.fetchIndexBlobs(ctx, indexBlobIDs)
		if err == nil {
			err = sm.committedContents.use(ctx, indexBlobIDs)
			if err != nil {
				return err
			}

			if len(indexBlobs) > indexBlobCompactionWarningThreshold {
				sm.log.Errorf("Found too many index blobs (%v), this may result in degraded performance.\n\nPlease ensure periodic repository maintenance is enabled or run 'kopia maintenance'.", len(indexBlobs))
			}

			return nil
		}

		if !errors.Is(err, blob.ErrBlobNotFound) {
			return err
		}
	}

	return errors.Errorf("unable to load pack indexes despite %v retries", indexLoadAttempts)
}

func (sm *SharedManager) getCacheForContentID(id ID) contentCache {
	if id.HasPrefix() {
		return sm.metadataCache
	}

	return sm.contentCache
}

func (sm *SharedManager) decryptContentAndVerify(payload []byte, bi Info) ([]byte, error) {
	sm.Stats.readContent(len(payload))

	var hashBuf [hashing.MaxHashSize]byte

	iv, err := getPackedContentIV(hashBuf[:], bi.GetContentID())
	if err != nil {
		return nil, err
	}

	// reserved for future use
	if k := bi.GetEncryptionKeyID(); k != 0 {
		return nil, errors.Errorf("unsupported encryption key ID: %v", k)
	}

	decrypted, err := sm.decryptAndVerify(payload, iv)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid checksum at %v offset %v length %v", bi.GetPackBlobID(), bi.GetPackOffset(), len(payload))
	}

	if h := bi.GetCompressionHeaderID(); h != 0 {
		c := compression.ByHeaderID[h]
		if c == nil {
			return nil, errors.Errorf("unsupported compressor %x", h)
		}

		out := bytes.NewBuffer(nil)

		if err := c.Decompress(out, decrypted); err != nil {
			return nil, errors.Wrap(err, "error decompressing")
		}

		return out.Bytes(), nil
	}

	return decrypted, nil
}

func (sm *SharedManager) decryptAndVerify(encrypted, iv []byte) ([]byte, error) {
	decrypted, err := sm.crypter.Encryptor.Decrypt(nil, encrypted, iv)
	if err != nil {
		sm.Stats.foundInvalidContent()
		return nil, errors.Wrap(err, "decrypt")
	}

	sm.Stats.foundValidContent()
	sm.Stats.decrypted(len(decrypted))

	// already verified
	return decrypted, nil
}

// IndexBlobs returns the list of active index blobs.
func (sm *SharedManager) IndexBlobs(ctx context.Context, includeInactive bool) ([]IndexBlobInfo, error) {
	if includeInactive {
		var result []IndexBlobInfo

		prefixes := []blob.ID{IndexBlobPrefix}

		for _, prefix := range prefixes {
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

	// nolint:wrapcheck
	return sm.indexBlobManager.listActiveIndexBlobs(ctx)
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

	return listcache.NewWrapper(st, cacheSt, cachedIndexBlobPrefixes, caching.HMACSecret, time.Duration(caching.MaxListCacheDurationSec)*time.Second), nil
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

	// nolint:wrapcheck
	return filesystem.New(ctx, &filesystem.Options{
		Path:            blobListCacheDir,
		DirectoryShards: []int{},
	})
}

func (sm *SharedManager) setupReadManagerCaches(ctx context.Context, caching *CachingOptions) error {
	dataCacheStorage, err := cache.NewStorageOrNil(ctx, caching.CacheDirectory, caching.MaxCacheSizeBytes, "contents")
	if err != nil {
		return errors.Wrap(err, "unable to initialize data cache storage")
	}

	dataCache, err := newContentCacheForData(ctx, sm.st, dataCacheStorage, caching.MaxCacheSizeBytes, caching.HMACSecret)
	if err != nil {
		return errors.Wrap(err, "unable to initialize content cache")
	}

	metadataCacheSize := caching.MaxMetadataCacheSizeBytes
	if metadataCacheSize == 0 && caching.MaxCacheSizeBytes > 0 {
		metadataCacheSize = caching.MaxCacheSizeBytes
	}

	metadataCacheStorage, err := cache.NewStorageOrNil(ctx, caching.CacheDirectory, metadataCacheSize, "metadata")
	if err != nil {
		return errors.Wrap(err, "unable to initialize data cache storage")
	}

	metadataCache, err := newContentCacheForMetadata(ctx, sm.st, metadataCacheStorage, metadataCacheSize)
	if err != nil {
		return errors.Wrap(err, "unable to initialize metadata cache")
	}

	ownWritesCachingSt, err := newOwnWritesCache(ctx, sm.st, caching)
	if err != nil {
		return errors.Wrap(err, "unable to initialize own writes cache")
	}

	cachedSt, err := newListCache(ctx, ownWritesCachingSt, caching)
	if err != nil {
		return errors.Wrap(err, "unable to initialize list cache")
	}

	sm.enc = &encryptedBlobMgr{
		st:             cachedSt,
		crypter:        sm.crypter,
		indexBlobCache: metadataCache,
		log:            logging.WithPrefix("[encrypted-blob-manager] ", sm.sharedBaseLogger),
	}

	sm.indexBlobManager = &indexBlobManagerV0{
		st:           cachedSt,
		enc:          sm.enc,
		timeNow:      sm.timeNow,
		maxPackSize:  sm.maxPackSize,
		indexVersion: sm.indexVersion,
		log:          logging.WithPrefix("[index-blob-manager] ", sm.sharedBaseLogger),
	}

	// once everything is ready, set it up
	sm.contentCache = dataCache
	sm.metadataCache = metadataCache
	sm.committedContents = newCommittedContentIndex(caching, uint32(sm.crypter.Encryptor.Overhead()), sm.indexVersion, sm.enc.getEncryptedBlob, sm.sharedBaseLogger)

	return nil
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

	sm.contentCache.close(ctx)
	sm.metadataCache.close(ctx)
	sm.encryptionBufferPool.Close()

	if sm.internalLogger != nil {
		sm.internalLogger.Close(ctx)
	}

	sm.internalLogManager.Close(ctx)

	return errors.Wrap(sm.st.Close(ctx), "error closing storage")
}

// InternalLogger returns the internal logger.
func (sm *SharedManager) InternalLogger() logging.Logger {
	return sm.internalLogger
}

// NewSharedManager returns SharedManager that is used by SessionWriteManagers on top of a repository.
func NewSharedManager(ctx context.Context, st blob.Storage, f *FormattingOptions, caching *CachingOptions, opts *ManagerOptions) (*SharedManager, error) {
	opts = opts.CloneOrDefault()
	if opts.TimeNow == nil {
		opts.TimeNow = clock.Now
	}

	if f.Version < minSupportedReadVersion || f.Version > currentWriteVersion {
		return nil, errors.Errorf("can't handle repositories created using version %v (min supported %v, max supported %v)", f.Version, minSupportedReadVersion, maxSupportedReadVersion)
	}

	if f.Version < minSupportedWriteVersion || f.Version > currentWriteVersion {
		return nil, errors.Errorf("can't handle repositories created using version %v (min supported %v, max supported %v)", f.Version, minSupportedWriteVersion, maxSupportedWriteVersion)
	}

	crypter, err := CreateCrypter(f)
	if err != nil {
		return nil, err
	}

	actualIndexVersion := f.IndexVersion
	if actualIndexVersion == 0 {
		actualIndexVersion = DefaultIndexVersion
	}

	if actualIndexVersion < v1IndexVersion || actualIndexVersion > v2IndexVersion {
		return nil, errors.Errorf("index version %v is not supported", actualIndexVersion)
	}

	// create internal logger that will be writing logs as encrypted repository blobs.
	ilm := newInternalLogManager(ctx, st, crypter)

	// sharedBaseLogger writes to the both context and internal log
	// and is used as a base for all content manager components.
	var internalLog *internalLogger

	// capture logger (usually console or log file) associated with current context.
	sharedBaseLogger := logging.GetContextLoggerFunc(FormatLogModule)(ctx)

	if !opts.DisableInternalLog {
		internalLog = ilm.NewLogger()
		sharedBaseLogger = logging.Broadcast{sharedBaseLogger, internalLog}
	}

	sm := &SharedManager{
		st:                      st,
		crypter:                 crypter,
		Stats:                   new(Stats),
		timeNow:                 opts.TimeNow,
		format:                  *f,
		maxPackSize:             f.MaxPackSize,
		minPreambleLength:       defaultMinPreambleLength,
		maxPreambleLength:       defaultMaxPreambleLength,
		paddingUnit:             defaultPaddingUnit,
		repositoryFormatBytes:   opts.RepositoryFormatBytes,
		checkInvariantsOnUnlock: os.Getenv("KOPIA_VERIFY_INVARIANTS") != "",
		writeFormatVersion:      int32(f.Version),
		encryptionBufferPool:    buf.NewPool(ctx, defaultEncryptionBufferPoolSegmentSize+crypter.Encryptor.Overhead()+maxCompressionOverheadPerContent, "content-manager-encryption"),
		indexVersion:            actualIndexVersion,
		internalLogManager:      ilm,
		internalLogger:          internalLog,
		sharedBaseLogger:        sharedBaseLogger,

		// remember logger defined for the context.
		log: logging.WithPrefix("[shared-manager] ", sharedBaseLogger),
	}

	caching = caching.CloneOrDefault()

	if err := sm.setupReadManagerCaches(ctx, caching); err != nil {
		return nil, errors.Wrap(err, "error setting up read manager caches")
	}

	if err := sm.loadPackIndexesUnlocked(ctx); err != nil {
		return nil, errors.Wrap(err, "error loading indexes")
	}

	return sm, nil
}
