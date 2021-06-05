package content

import (
	"bytes"
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/buf"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/logging"
)

// number of bytes to read from each pack index when recovering the index.
// per-pack indexes are usually short (<100-200 contents).
const indexRecoverPostambleSize = 8192

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

func (sm *SharedManager) loadPackIndexesUnlocked(ctx context.Context) ([]IndexBlobInfo, error) {
	nextSleepTime := 100 * time.Millisecond //nolint:gomnd

	for i := 0; i < indexLoadAttempts; i++ {
		if err := ctx.Err(); err != nil {
			// nolint:wrapcheck
			return nil, err
		}

		if i > 0 {
			sm.indexBlobManager.flushCache()
			sm.log.Debugf("encountered NOT_FOUND when loading, sleeping %v before retrying #%v", nextSleepTime, i)
			time.Sleep(nextSleepTime)
			nextSleepTime *= 2
		}

		indexBlobs, err := sm.indexBlobManager.listIndexBlobs(ctx, false)
		if err != nil {
			return nil, errors.Wrap(err, "error listing index blobs")
		}

		err = sm.tryLoadPackIndexBlobsUnlocked(ctx, indexBlobs)
		if err == nil {
			var indexBlobIDs []blob.ID
			for _, b := range indexBlobs {
				indexBlobIDs = append(indexBlobIDs, b.BlobID)
			}

			err = sm.committedContents.use(ctx, indexBlobIDs)
			if err != nil {
				return nil, err
			}

			if len(indexBlobs) > indexBlobCompactionWarningThreshold {
				sm.log.Errorf("Found too many index blobs (%v), this may result in degraded performance.\n\nPlease ensure periodic repository maintenance is enabled or run 'kopia maintenance'.", len(indexBlobs))
			}

			return indexBlobs, nil
		}

		if !errors.Is(err, blob.ErrBlobNotFound) {
			return nil, err
		}
	}

	return nil, errors.Errorf("unable to load pack indexes despite %v retries", indexLoadAttempts)
}

func (sm *SharedManager) tryLoadPackIndexBlobsUnlocked(ctx context.Context, indexBlobs []IndexBlobInfo) error {
	ch, unprocessedIndexesSize, err := sm.unprocessedIndexBlobsUnlocked(ctx, indexBlobs)
	if err != nil {
		return err
	}

	if len(ch) == 0 {
		return nil
	}

	sm.log.Debugf("downloading %v new index blobs (%v bytes)...", len(ch), unprocessedIndexesSize)

	var wg sync.WaitGroup

	errch := make(chan error, parallelFetches)

	for i := 0; i < parallelFetches; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for indexBlobID := range ch {
				data, err := sm.indexBlobManager.getIndexBlob(ctx, indexBlobID)
				if err != nil {
					errch <- err
					return
				}

				if err := sm.committedContents.addContent(ctx, indexBlobID, data, false); err != nil {
					errch <- errors.Wrap(err, "unable to add to committed content cache")
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errch)

	// Propagate async errors, if any.
	for err := range errch {
		return err
	}

	sm.log.Debugf("Index contents downloaded.")

	return nil
}

// unprocessedIndexBlobsUnlocked returns a closed channel filled with content IDs that are not in committedContents cache.
func (sm *SharedManager) unprocessedIndexBlobsUnlocked(ctx context.Context, contents []IndexBlobInfo) (resultCh <-chan blob.ID, totalSize int64, err error) {
	ch := make(chan blob.ID, len(contents))
	defer close(ch)

	for _, c := range contents {
		has, err := sm.committedContents.cache.hasIndexBlobID(ctx, c.BlobID)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "error determining whether index blob %v has been downloaded", c.BlobID)
		}

		if has {
			sm.log.Debugf("index-already-cached %v", c.BlobID)
			continue
		}

		ch <- c.BlobID
		totalSize += c.Length
	}

	return ch, totalSize, nil
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
	// nolint:wrapcheck
	return sm.indexBlobManager.listIndexBlobs(ctx, includeInactive)
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

	listCache, err := newListCache(sm.st, caching, sm.sharedBaseLogger)
	if err != nil {
		return errors.Wrap(err, "unable to initialize list cache")
	}

	// this is test action to allow test to specify custom cache
	owc, err := newOwnWritesCache(ctx, caching, sm.timeNow, sm.sharedBaseLogger)
	if err != nil {
		return errors.Wrap(err, "unable to initialize own writes cache")
	}

	contentIndex := newCommittedContentIndex(caching, uint32(sm.crypter.Encryptor.Overhead()), sm.indexVersion, sm.sharedBaseLogger)

	// once everything is ready, set it up
	sm.contentCache = dataCache
	sm.metadataCache = metadataCache
	sm.committedContents = contentIndex

	sm.indexBlobManager = &indexBlobManagerImpl{
		st:             sm.st,
		crypter:        sm.crypter,
		timeNow:        sm.timeNow,
		ownWritesCache: owc,
		listCache:      listCache,
		indexBlobCache: metadataCache,
		log:            logging.WithPrefix("[index-blob-manager] ", sm.sharedBaseLogger),
	}

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

	if _, err := sm.loadPackIndexesUnlocked(ctx); err != nil {
		return nil, errors.Wrap(err, "error loading indexes")
	}

	return sm, nil
}
