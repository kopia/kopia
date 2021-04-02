package content

import (
	"context"
	"crypto/aes"
	cryptorand "crypto/rand"
	"encoding/hex"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

const indexBlobCompactionWarningThreshold = 1000

func (sm *SharedManager) maybeEncryptContentDataForPacking(output *gather.WriteBuffer, data []byte, contentID ID) error {
	var hashOutput [maxHashSize]byte

	iv, err := getPackedContentIV(hashOutput[:], contentID)
	if err != nil {
		return errors.Wrapf(err, "unable to get packed content IV for %q", contentID)
	}

	b := sm.encryptionBufferPool.Allocate(len(data) + sm.encryptor.MaxOverhead())
	defer b.Release()

	cipherText, err := sm.encryptor.Encrypt(b.Data[:0], data, iv)
	if err != nil {
		return errors.Wrap(err, "unable to encrypt")
	}

	sm.Stats.encrypted(len(data))

	output.Append(cipherText)

	return nil
}

func writeRandomBytesToBuffer(b *gather.WriteBuffer, count int) error {
	var rnd [defaultPaddingUnit]byte

	if _, err := io.ReadFull(cryptorand.Reader, rnd[0:count]); err != nil {
		return errors.Wrap(err, "error getting random bytes")
	}

	b.Append(rnd[0:count])

	return nil
}

// ValidatePrefix returns an error if a given prefix is invalid.
func ValidatePrefix(prefix ID) error {
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

func (bm *WriteManager) getContentDataUnlocked(ctx context.Context, pp *pendingPackInfo, bi *Info) ([]byte, error) {
	var payload []byte

	if pp != nil && pp.packBlobID == bi.PackBlobID {
		// we need to use a lock here in case somebody else writes to the pack at the same time.
		payload = pp.currentPackData.AppendSectionTo(nil, int(bi.PackOffset), int(bi.Length))
	} else {
		var err error

		payload, err = bm.getCacheForContentID(bi.ID).getContent(ctx, cacheKey(bi.ID), bi.PackBlobID, int64(bi.PackOffset), int64(bi.Length))
		if err != nil {
			return nil, errors.Wrap(err, "getCacheForContentID")
		}
	}

	return bm.decryptContentAndVerify(payload, bi)
}

func (bm *WriteManager) preparePackDataContent(ctx context.Context, pp *pendingPackInfo) (packIndexBuilder, error) {
	packFileIndex := packIndexBuilder{}
	haveContent := false

	for _, info := range pp.currentPackItems {
		if info.PackBlobID == pp.packBlobID {
			haveContent = true
		}

		formatLog(ctx).Debugf("add-to-pack %v %v p:%v %v d:%v", pp.packBlobID, info.ID, info.PackBlobID, info.Length, info.Deleted)

		packFileIndex.Add(info)
	}

	if len(packFileIndex) == 0 {
		return nil, nil
	}

	if !haveContent {
		// we wrote pack preamble but no actual content, revert it
		pp.currentPackData.Reset()
		return packFileIndex, nil
	}

	if pp.finalized {
		return packFileIndex, nil
	}

	pp.finalized = true

	if bm.paddingUnit > 0 {
		if missing := bm.paddingUnit - (pp.currentPackData.Length() % bm.paddingUnit); missing > 0 {
			if err := writeRandomBytesToBuffer(pp.currentPackData, missing); err != nil {
				return nil, errors.Wrap(err, "unable to prepare content postamble")
			}
		}
	}

	err := bm.writePackFileIndexRecoveryData(pp.currentPackData, packFileIndex)

	return packFileIndex, err
}

func getPackedContentIV(output []byte, contentID ID) ([]byte, error) {
	n, err := hex.Decode(output, []byte(contentID[len(contentID)-(aes.BlockSize*2):])) //nolint:gomnd
	if err != nil {
		return nil, errors.Wrapf(err, "error decoding content IV from %v", contentID)
	}

	return output[0:n], nil
}

func (bm *WriteManager) writePackFileNotLocked(ctx context.Context, packFile blob.ID, data gather.Bytes) error {
	bm.Stats.wroteContent(data.Length())
	bm.onUpload(int64(data.Length()))

	return bm.st.PutBlob(ctx, packFile, data)
}

func (sm *SharedManager) hashData(output, data []byte) []byte {
	// Hash the content and compute encryption key.
	contentID := sm.hasher(output, data)
	sm.Stats.hashedContent(len(data))

	return contentID
}

// CreateHashAndEncryptor returns new hashing and encrypting functions based on
// the specified formatting options.
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
