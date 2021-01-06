package content

import (
	"bytes"
	"context"
	"crypto/aes"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

// CommittedReadManager is responsible for read-only access to committed data.
type CommittedReadManager struct {
	Stats             *Stats
	st                blob.Storage
	indexBlobManager  indexBlobManager
	contentCache      contentCache
	metadataCache     contentCache
	committedContents *committedContentIndex
	hasher            hashing.HashFunc
	encryptor         encryption.Encryptor
	timeNow           func() time.Time
}

func (rm *CommittedReadManager) readPackFileLocalIndex(ctx context.Context, packFile blob.ID, packFileLength int64) ([]byte, error) {
	// TODO(jkowalski): optimize read when packFileLength is provided
	_ = packFileLength

	payload, err := rm.st.GetBlob(ctx, packFile, 0, -1)
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

	localIndexBytes, err := rm.decryptAndVerify(encryptedLocalIndexBytes, postamble.localIndexIV)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decrypt local index")
	}

	return localIndexBytes, nil
}

func (rm *CommittedReadManager) loadPackIndexesUnlocked(ctx context.Context) ([]IndexBlobInfo, bool, error) {
	nextSleepTime := 100 * time.Millisecond //nolint:gomnd

	for i := 0; i < indexLoadAttempts; i++ {
		if err := ctx.Err(); err != nil {
			// nolint:wrapcheck
			return nil, false, err
		}

		if i > 0 {
			rm.indexBlobManager.flushCache()
			log(ctx).Debugf("encountered NOT_FOUND when loading, sleeping %v before retrying #%v", nextSleepTime, i)
			time.Sleep(nextSleepTime)
			nextSleepTime *= 2
		}

		indexBlobs, err := rm.indexBlobManager.listIndexBlobs(ctx, false)
		if err != nil {
			return nil, false, errors.Wrap(err, "error listing index blobs")
		}

		err = rm.tryLoadPackIndexBlobsUnlocked(ctx, indexBlobs)
		if err == nil {
			var indexBlobIDs []blob.ID
			for _, b := range indexBlobs {
				indexBlobIDs = append(indexBlobIDs, b.BlobID)
			}

			var updated bool

			updated, err = rm.committedContents.use(ctx, indexBlobIDs)
			if err != nil {
				return nil, false, err
			}

			if len(indexBlobs) > indexBlobCompactionWarningThreshold {
				log(ctx).Warningf("Found too many index blobs (%v), this may result in degraded performance.\n\nPlease ensure periodic repository maintenance is enabled or run 'kopia maintenance'.", len(indexBlobs))
			}

			return indexBlobs, updated, nil
		}

		if !errors.Is(err, blob.ErrBlobNotFound) {
			return nil, false, err
		}
	}

	return nil, false, errors.Errorf("unable to load pack indexes despite %v retries", indexLoadAttempts)
}

func (rm *CommittedReadManager) tryLoadPackIndexBlobsUnlocked(ctx context.Context, indexBlobs []IndexBlobInfo) error {
	ch, unprocessedIndexesSize, err := rm.unprocessedIndexBlobsUnlocked(ctx, indexBlobs)
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
				data, err := rm.indexBlobManager.getIndexBlob(ctx, indexBlobID)
				if err != nil {
					errch <- err
					return
				}

				if err := rm.committedContents.addContent(ctx, indexBlobID, data, false); err != nil {
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
func (rm *CommittedReadManager) unprocessedIndexBlobsUnlocked(ctx context.Context, contents []IndexBlobInfo) (resultCh <-chan blob.ID, totalSize int64, err error) {
	ch := make(chan blob.ID, len(contents))
	defer close(ch)

	for _, c := range contents {
		has, err := rm.committedContents.cache.hasIndexBlobID(ctx, c.BlobID)
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

func (rm *CommittedReadManager) getCacheForContentID(id ID) contentCache {
	if id.HasPrefix() {
		return rm.metadataCache
	}

	return rm.contentCache
}

func (rm *CommittedReadManager) decryptContentAndVerify(payload []byte, bi *Info) ([]byte, error) {
	rm.Stats.readContent(len(payload))

	var hashBuf [maxHashSize]byte

	iv, err := getPackedContentIV(hashBuf[:], bi.ID)
	if err != nil {
		return nil, err
	}

	decrypted, err := rm.decryptAndVerify(payload, iv)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid checksum at %v offset %v length %v", bi.PackBlobID, bi.PackOffset, len(payload))
	}

	return decrypted, nil
}

func (rm *CommittedReadManager) decryptAndVerify(encrypted, iv []byte) ([]byte, error) {
	decrypted, err := rm.encryptor.Decrypt(nil, encrypted, iv)
	if err != nil {
		return nil, errors.Wrap(err, "decrypt")
	}

	rm.Stats.decrypted(len(decrypted))

	if rm.encryptor.IsAuthenticated() {
		// already verified
		return decrypted, nil
	}

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	return decrypted, rm.verifyChecksum(decrypted, iv)
}

// IndexBlobs returns the list of active index blobs.
func (rm *CommittedReadManager) IndexBlobs(ctx context.Context, includeInactive bool) ([]IndexBlobInfo, error) {
	return rm.indexBlobManager.listIndexBlobs(ctx, includeInactive)
}

func (rm *CommittedReadManager) verifyChecksum(data, contentID []byte) error {
	var hashOutput [maxHashSize]byte

	expected := rm.hasher(hashOutput[:0], data)
	expected = expected[len(expected)-aes.BlockSize:]

	if !bytes.HasSuffix(contentID, expected) {
		rm.Stats.foundInvalidContent()
		return errors.Errorf("invalid checksum for blob %x, expected %x", contentID, expected)
	}

	rm.Stats.foundValidContent()

	return nil
}

func (rm *CommittedReadManager) setupReadManagerCaches(ctx context.Context, caching *CachingOptions) error {
	dataCacheStorage, err := newCacheStorageOrNil(ctx, caching.CacheDirectory, caching.MaxCacheSizeBytes, "contents")
	if err != nil {
		return errors.Wrap(err, "unable to initialize data cache storage")
	}

	dataCache, err := newContentCacheForData(ctx, rm.st, dataCacheStorage, caching.MaxCacheSizeBytes, caching.HMACSecret)
	if err != nil {
		return errors.Wrap(err, "unable to initialize content cache")
	}

	metadataCacheSize := caching.MaxMetadataCacheSizeBytes
	if metadataCacheSize == 0 && caching.MaxCacheSizeBytes > 0 {
		metadataCacheSize = caching.MaxCacheSizeBytes
	}

	metadataCacheStorage, err := newCacheStorageOrNil(ctx, caching.CacheDirectory, metadataCacheSize, "metadata")
	if err != nil {
		return errors.Wrap(err, "unable to initialize data cache storage")
	}

	metadataCache, err := newContentCacheForMetadata(ctx, rm.st, metadataCacheStorage, metadataCacheSize)
	if err != nil {
		return errors.Wrap(err, "unable to initialize metadata cache")
	}

	listCache, err := newListCache(rm.st, caching)
	if err != nil {
		return errors.Wrap(err, "unable to initialize list cache")
	}

	// this is test action to allow test to specify custom cache
	owc, err := newOwnWritesCache(ctx, caching, rm.timeNow)
	if err != nil {
		return errors.Wrap(err, "unable to initialize own writes cache")
	}

	contentIndex := newCommittedContentIndex(caching)

	// once everything is ready, set it up
	rm.contentCache = dataCache
	rm.metadataCache = metadataCache
	rm.committedContents = contentIndex

	rm.indexBlobManager = &indexBlobManagerImpl{
		st:                               rm.st,
		encryptor:                        rm.encryptor,
		hasher:                           rm.hasher,
		timeNow:                          rm.timeNow,
		ownWritesCache:                   owc,
		listCache:                        listCache,
		indexBlobCache:                   metadataCache,
		maxEventualConsistencySettleTime: defaultEventualConsistencySettleTime,
	}

	return nil
}

func newReadManager(ctx context.Context, st blob.Storage, f *FormattingOptions, caching *CachingOptions, opts *ManagerOptions) (*CommittedReadManager, error) {
	hasher, encryptor, err := CreateHashAndEncryptor(f)
	if err != nil {
		return nil, err
	}

	rm := &CommittedReadManager{
		st:        st,
		encryptor: encryptor,
		hasher:    hasher,
		Stats:     new(Stats),
		timeNow:   opts.TimeNow,
	}

	caching = caching.CloneOrDefault()

	if err := rm.setupReadManagerCaches(ctx, caching); err != nil {
		return nil, errors.Wrap(err, "error setting up read manager caches")
	}

	return rm, nil
}
