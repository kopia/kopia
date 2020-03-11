package content

import (
	"bytes"
	"context"
	"crypto/aes"
	cryptorand "crypto/rand"
	"encoding/hex"
	"io"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

// lockFreeManager contains parts of Manager state that can be accessed without locking
type lockFreeManager struct {
	// this one is not lock-free
	Stats Stats

	listCache      *listCache
	st             blob.Storage
	Format         FormattingOptions
	CachingOptions CachingOptions

	contentCache      *contentCache
	metadataCache     *contentCache
	committedContents *committedContentIndex

	checkInvariantsOnUnlock bool

	writeFormatVersion int32 // format version to write

	maxPackSize       int
	hasher            hashing.HashFunc
	encryptor         encryption.Encryptor
	minPreambleLength int
	maxPreambleLength int
	paddingUnit       int
	timeNow           func() time.Time

	repositoryFormatBytes []byte
}

func (bm *lockFreeManager) maybeEncryptContentDataForPacking(output, data []byte, contentID ID) ([]byte, error) {
	iv, err := getPackedContentIV(contentID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get packed content IV for %q", contentID)
	}

	return bm.encryptor.Encrypt(output, data, iv)
}

func appendRandomBytes(b []byte, count int) ([]byte, error) {
	rnd := make([]byte, count)
	if _, err := io.ReadFull(cryptorand.Reader, rnd); err != nil {
		return nil, err
	}

	return append(b, rnd...), nil
}

func (bm *lockFreeManager) loadPackIndexesUnlocked(ctx context.Context) ([]IndexBlobInfo, bool, error) {
	nextSleepTime := 100 * time.Millisecond //nolint:gomnd

	for i := 0; i < indexLoadAttempts; i++ {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}

		if i > 0 {
			bm.listCache.deleteListCache()
			log(ctx).Debugf("encountered NOT_FOUND when loading, sleeping %v before retrying #%v", nextSleepTime, i)
			time.Sleep(nextSleepTime)
			nextSleepTime *= 2
		}

		contents, err := bm.listCache.listIndexBlobs(ctx)
		if err != nil {
			return nil, false, err
		}

		err = bm.tryLoadPackIndexBlobsUnlocked(ctx, contents)
		if err == nil {
			var contentIDs []blob.ID
			for _, b := range contents {
				contentIDs = append(contentIDs, b.BlobID)
			}

			var updated bool

			updated, err = bm.committedContents.use(ctx, contentIDs)
			if err != nil {
				return nil, false, err
			}

			return contents, updated, nil
		}

		if err != blob.ErrBlobNotFound {
			return nil, false, err
		}
	}

	return nil, false, errors.Errorf("unable to load pack indexes despite %v retries", indexLoadAttempts)
}

func (bm *lockFreeManager) tryLoadPackIndexBlobsUnlocked(ctx context.Context, contents []IndexBlobInfo) error {
	ch, unprocessedIndexesSize, err := bm.unprocessedIndexBlobsUnlocked(ctx, contents)
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
				data, err := bm.getIndexBlobInternal(ctx, indexBlobID)
				if err != nil {
					errch <- err
					return
				}

				if err := bm.committedContents.addContent(ctx, indexBlobID, data, false); err != nil {
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
func (bm *lockFreeManager) unprocessedIndexBlobsUnlocked(ctx context.Context, contents []IndexBlobInfo) (resultCh <-chan blob.ID, totalSize int64, err error) {
	ch := make(chan blob.ID, len(contents))
	defer close(ch)

	for _, c := range contents {
		has, err := bm.committedContents.cache.hasIndexBlobID(ctx, c.BlobID)
		if err != nil {
			return nil, 0, err
		}

		if has {
			log(ctx).Debugf("index blob %q already in cache, skipping", c.BlobID)
			continue
		}

		ch <- c.BlobID
		totalSize += c.Length
	}

	return ch, totalSize, nil
}

func validatePrefix(prefix ID) error {
	switch len(prefix) {
	case 0:
		return nil
	case 1:
		if prefix[0] >= 'g' && prefix[0] <= 'z' {
			return nil
		}
	}

	return errors.Errorf("invalid prefix, must be a empty or single letter between 'g' and 'z'")
}

func cloneBytes(b []byte) []byte {
	return append([]byte{}, b...)
}

func (bm *lockFreeManager) getCacheForContentID(id ID) *contentCache {
	if id.HasPrefix() {
		return bm.metadataCache
	}

	return bm.contentCache
}

func (bm *lockFreeManager) getContentDataUnlocked(ctx context.Context, bi *Info) ([]byte, error) {
	if bi.Payload != nil {
		return cloneBytes(bi.Payload), nil
	}

	payload, err := bm.getCacheForContentID(bi.ID).getContent(ctx, cacheKey(bi.ID), bi.PackBlobID, int64(bi.PackOffset), int64(bi.Length))
	if err != nil {
		return nil, err
	}

	bm.Stats.readContent(len(payload))

	iv, err := getPackedContentIV(bi.ID)
	if err != nil {
		return nil, err
	}

	decrypted, err := bm.decryptAndVerify(payload, iv)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid checksum at %v offset %v length %v", bi.PackBlobID, bi.PackOffset, len(payload))
	}

	return decrypted, nil
}

func (bm *lockFreeManager) decryptAndVerify(encrypted, iv []byte) ([]byte, error) {
	decrypted, err := bm.encryptor.Decrypt(nil, encrypted, iv)
	if err != nil {
		return nil, errors.Wrap(err, "decrypt")
	}

	bm.Stats.decrypted(len(decrypted))

	if bm.encryptor.IsAuthenticated() {
		// already verified
		return decrypted, nil
	}

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	return decrypted, bm.verifyChecksum(decrypted, iv)
}

func (bm *lockFreeManager) preparePackDataContent(ctx context.Context, contentData []byte, pp *pendingPackInfo, packFile blob.ID) ([]byte, packIndexBuilder, error) {
	formatLog(ctx).Debugf("preparing content data with %v items", len(pp.currentPackItems))

	contentData, err := appendRandomBytes(append(contentData, bm.repositoryFormatBytes...), rand.Intn(bm.maxPreambleLength-bm.minPreambleLength+1)+bm.minPreambleLength)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to prepare content preamble")
	}

	packFileIndex := packIndexBuilder{}
	haveContent := false

	var encryptedTmp []byte

	for contentID, info := range pp.currentPackItems {
		if info.Payload == nil {
			// no payload, it's a deletion of a previously-committed content.
			packFileIndex.Add(info)
			continue
		}

		haveContent = true

		encryptedTmp, err = bm.maybeEncryptContentDataForPacking(encryptedTmp[:0], info.Payload, info.ID)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "unable to encrypt %q", info.ID)
		}

		formatLog(ctx).Debugf("adding %v length=%v deleted=%v", contentID, len(info.Payload), info.Deleted)

		packFileIndex.Add(Info{
			ID:               contentID,
			Deleted:          info.Deleted,
			FormatVersion:    byte(bm.writeFormatVersion),
			PackBlobID:       packFile,
			PackOffset:       uint32(len(contentData)),
			Length:           uint32(len(encryptedTmp)),
			TimestampSeconds: info.TimestampSeconds,
		})

		if contentID.HasPrefix() {
			bm.metadataCache.put(ctx, cacheKey(contentID), cloneBytes(encryptedTmp))
		}

		contentData = append(contentData, encryptedTmp...)
	}

	if len(packFileIndex) == 0 {
		return nil, nil, nil
	}

	if !haveContent {
		return nil, packFileIndex, nil
	}

	if bm.paddingUnit > 0 {
		if missing := bm.paddingUnit - (len(contentData) % bm.paddingUnit); missing > 0 {
			contentData, err = appendRandomBytes(contentData, missing)
			if err != nil {
				return nil, nil, errors.Wrap(err, "unable to prepare content postamble")
			}
		}
	}

	origContentLength := len(contentData)
	contentData, err = bm.appendPackFileIndexRecoveryData(ctx, contentData, packFileIndex)

	formatLog(ctx).Debugf("finished content %v bytes (%v bytes index)", len(contentData), len(contentData)-origContentLength)

	return contentData, packFileIndex, err
}

// IndexBlobs returns the list of active index blobs.
func (bm *lockFreeManager) IndexBlobs(ctx context.Context) ([]IndexBlobInfo, error) {
	return bm.listCache.listIndexBlobs(ctx)
}

func (bm *lockFreeManager) getIndexBlobInternal(ctx context.Context, blobID blob.ID) ([]byte, error) {
	payload, err := bm.contentCache.getContent(ctx, cacheKey(blobID), blobID, 0, -1)
	if err != nil {
		return nil, err
	}

	iv, err := getIndexBlobIV(blobID)
	if err != nil {
		return nil, err
	}

	bm.Stats.readContent(len(payload))

	payload, err = bm.encryptor.Decrypt(nil, payload, iv)
	bm.Stats.decrypted(len(payload))

	if err != nil {
		return nil, err
	}

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	if err := bm.verifyChecksum(payload, iv); err != nil {
		return nil, err
	}

	return payload, nil
}

func getPackedContentIV(contentID ID) ([]byte, error) {
	return hex.DecodeString(string(contentID[len(contentID)-(aes.BlockSize*2):]))
}

func getIndexBlobIV(s blob.ID) ([]byte, error) {
	if p := strings.Index(string(s), "-"); p >= 0 { // nolint:gocritic
		s = s[0:p]
	}

	return hex.DecodeString(string(s[len(s)-(aes.BlockSize*2):]))
}

func (bm *lockFreeManager) writePackFileNotLocked(ctx context.Context, packFile blob.ID, data []byte) error {
	bm.Stats.wroteContent(len(data))
	bm.listCache.deleteListCache()

	return bm.st.PutBlob(ctx, packFile, data)
}

func (bm *lockFreeManager) encryptAndWriteBlobNotLocked(ctx context.Context, data []byte, prefix blob.ID) (blob.ID, error) {
	var hashOutput [maxHashSize]byte

	hash := bm.hashData(hashOutput[:0], data)
	blobID := prefix + blob.ID(hex.EncodeToString(hash))

	bm.Stats.encrypted(len(data))

	data2, err := bm.encryptor.Encrypt(nil, data, hash)
	if err != nil {
		return "", err
	}

	bm.Stats.wroteContent(len(data2))
	bm.listCache.deleteListCache()

	if err := bm.st.PutBlob(ctx, blobID, data2); err != nil {
		return "", err
	}

	return blobID, nil
}

func (bm *lockFreeManager) hashData(output, data []byte) []byte {
	// Hash the content and compute encryption key.
	contentID := bm.hasher(output, data)
	bm.Stats.hashedContent(len(data))

	return contentID
}

func (bm *lockFreeManager) writePackIndexesNew(ctx context.Context, data []byte) (blob.ID, error) {
	return bm.encryptAndWriteBlobNotLocked(ctx, data, newIndexBlobPrefix)
}

func (bm *lockFreeManager) verifyChecksum(data, contentID []byte) error {
	var hashOutput [maxHashSize]byte

	expected := bm.hasher(hashOutput[:0], data)
	expected = expected[len(expected)-aes.BlockSize:]

	if !bytes.HasSuffix(contentID, expected) {
		bm.Stats.foundInvalidContent()
		return errors.Errorf("invalid checksum for blob %x, expected %x", contentID, expected)
	}

	bm.Stats.foundValidContent()

	return nil
}

// CreateHashAndEncryptor returns new hashing and encrypting functions based on
// the specified formatting options
func CreateHashAndEncryptor(f *FormattingOptions) (hashing.HashFunc, encryption.Encryptor, error) {
	h, err := hashing.CreateHashFunc(f)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create hash")
	}

	e, err := encryption.CreateEncryptor(f)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create encryptor")
	}

	contentID := h(nil, nil)

	_, err = e.Encrypt(nil, nil, contentID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "invalid encryptor")
	}

	return h, e, nil
}
