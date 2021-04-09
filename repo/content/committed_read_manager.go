package content

import (
	"bytes"
	"context"
	"crypto/aes"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/buf"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

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
	hasher            hashing.HashFunc
	encryptor         encryption.Encryptor
	timeNow           func() time.Time

	format                  FormattingOptions
	checkInvariantsOnUnlock bool
	writeFormatVersion      int32 // format version to write
	maxPackSize             int
	minPreambleLength       int
	maxPreambleLength       int
	paddingUnit             int
	repositoryFormatBytes   []byte

	encryptionBufferPool *buf.Pool
}

func (sm *SharedManager) readPackFileLocalIndex(ctx context.Context, packFile blob.ID, packFileLength int64) ([]byte, error) {
	// TODO(jkowalski): optimize read when packFileLength is provided
	_ = packFileLength

	payload, err := sm.st.GetBlob(ctx, packFile, 0, -1)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting blob %v", packFile)
	}

	postamble := findPostamble(payload)
	if postamble == nil {
		return nil, errors.Errorf("unable to find valid postamble in file %v", packFile)
	}

	if uint64(postamble.localIndexOffset+postamble.localIndexLength) > uint64(len(payload)) {
		// invalid offset/length
		return nil, errors.Errorf("unable to find valid local index in file %v", packFile)
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

func (sm *SharedManager) loadPackIndexesUnlocked(ctx context.Context) ([]IndexBlobInfo, bool, error) {
	nextSleepTime := 100 * time.Millisecond //nolint:gomnd

	for i := 0; i < indexLoadAttempts; i++ {
		if err := ctx.Err(); err != nil {
			// nolint:wrapcheck
			return nil, false, err
		}

		if i > 0 {
			sm.indexBlobManager.flushCache()
			log(ctx).Debugf("encountered NOT_FOUND when loading, sleeping %v before retrying #%v", nextSleepTime, i)
			time.Sleep(nextSleepTime)
			nextSleepTime *= 2
		}

		indexBlobs, err := sm.indexBlobManager.listIndexBlobs(ctx, false)
		if err != nil {
			return nil, false, errors.Wrap(err, "error listing index blobs")
		}

		err = sm.tryLoadPackIndexBlobsUnlocked(ctx, indexBlobs)
		if err == nil {
			var indexBlobIDs []blob.ID
			for _, b := range indexBlobs {
				indexBlobIDs = append(indexBlobIDs, b.BlobID)
			}

			var updated bool

			updated, err = sm.committedContents.use(ctx, indexBlobIDs)
			if err != nil {
				return nil, false, err
			}

			if len(indexBlobs) > indexBlobCompactionWarningThreshold {
				log(ctx).Errorf("Found too many index blobs (%v), this may result in degraded performance.\n\nPlease ensure periodic repository maintenance is enabled or run 'kopia maintenance'.", len(indexBlobs))
			}

			return indexBlobs, updated, nil
		}

		if !errors.Is(err, blob.ErrBlobNotFound) {
			return nil, false, err
		}
	}

	return nil, false, errors.Errorf("unable to load pack indexes despite %v retries", indexLoadAttempts)
}

func (sm *SharedManager) tryLoadPackIndexBlobsUnlocked(ctx context.Context, indexBlobs []IndexBlobInfo) error {
	ch, unprocessedIndexesSize, err := sm.unprocessedIndexBlobsUnlocked(ctx, indexBlobs)
	if err != nil {
		return err
	}

	if len(ch) == 0 {
		return nil
	}

	log(ctx).Debugf("downloading %v new index blobs (%v bytes)...", len(ch), unprocessedIndexesSize)

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

	log(ctx).Debugf("Index contents downloaded.")

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
			formatLog(ctx).Debugf("index-already-cached %v", c.BlobID)
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

func (sm *SharedManager) decryptContentAndVerify(payload []byte, bi *Info) ([]byte, error) {
	sm.Stats.readContent(len(payload))

	var hashBuf [maxHashSize]byte

	iv, err := getPackedContentIV(hashBuf[:], bi.ID)
	if err != nil {
		return nil, err
	}

	decrypted, err := sm.decryptAndVerify(payload, iv)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid checksum at %v offset %v length %v", bi.PackBlobID, bi.PackOffset, len(payload))
	}

	return decrypted, nil
}

func (sm *SharedManager) decryptAndVerify(encrypted, iv []byte) ([]byte, error) {
	decrypted, err := sm.encryptor.Decrypt(nil, encrypted, iv)
	if err != nil {
		return nil, errors.Wrap(err, "decrypt")
	}

	sm.Stats.decrypted(len(decrypted))

	if sm.encryptor.IsAuthenticated() {
		// already verified
		return decrypted, nil
	}

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	return decrypted, sm.verifyChecksum(decrypted, iv)
}

// IndexBlobs returns the list of active index blobs.
func (sm *SharedManager) IndexBlobs(ctx context.Context, includeInactive bool) ([]IndexBlobInfo, error) {
	return sm.indexBlobManager.listIndexBlobs(ctx, includeInactive)
}

func (sm *SharedManager) verifyChecksum(data, contentID []byte) error {
	var hashOutput [maxHashSize]byte

	expected := sm.hasher(hashOutput[:0], data)
	expected = expected[len(expected)-aes.BlockSize:]

	if !bytes.HasSuffix(contentID, expected) {
		sm.Stats.foundInvalidContent()
		return errors.Errorf("invalid checksum for blob %x, expected %x", contentID, expected)
	}

	sm.Stats.foundValidContent()

	return nil
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

	listCache, err := newListCache(sm.st, caching)
	if err != nil {
		return errors.Wrap(err, "unable to initialize list cache")
	}

	// this is test action to allow test to specify custom cache
	owc, err := newOwnWritesCache(ctx, caching, sm.timeNow)
	if err != nil {
		return errors.Wrap(err, "unable to initialize own writes cache")
	}

	contentIndex := newCommittedContentIndex(caching)

	// once everything is ready, set it up
	sm.contentCache = dataCache
	sm.metadataCache = metadataCache
	sm.committedContents = contentIndex

	sm.indexBlobManager = &indexBlobManagerImpl{
		st:             sm.st,
		encryptor:      sm.encryptor,
		hasher:         sm.hasher,
		timeNow:        sm.timeNow,
		ownWritesCache: owc,
		listCache:      listCache,
		indexBlobCache: metadataCache,
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
	remaining := atomic.AddInt32(&sm.refCount, -1)
	if remaining != 0 {
		log(ctx).Debugf("not closing shared manager, remaining = %v", remaining)
		return nil
	}

	atomic.StoreInt32(&sm.closed, 1)

	log(ctx).Debugf("closing shared manager")

	if err := sm.committedContents.close(); err != nil {
		return errors.Wrap(err, "error closed committed content index")
	}

	sm.contentCache.close(ctx)
	sm.metadataCache.close(ctx)
	sm.encryptionBufferPool.Close()

	return sm.st.Close(ctx)
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

	hasher, encryptor, err := CreateHashAndEncryptor(f)
	if err != nil {
		return nil, err
	}

	sm := &SharedManager{
		st:                      st,
		encryptor:               encryptor,
		hasher:                  hasher,
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
		encryptionBufferPool:    buf.NewPool(ctx, defaultEncryptionBufferPoolSegmentSize+encryptor.MaxOverhead(), "content-manager-encryption"),
	}

	caching = caching.CloneOrDefault()

	if err := sm.setupReadManagerCaches(ctx, caching); err != nil {
		return nil, errors.Wrap(err, "error setting up read manager caches")
	}

	if _, _, err := sm.loadPackIndexesUnlocked(ctx); err != nil {
		return nil, errors.Wrap(err, "error loading indexes")
	}

	return sm, nil
}
