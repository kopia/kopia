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
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

const indexBlobCompactionWarningThreshold = 1000

func (sm *SharedManager) maybeCompressAndEncryptDataForPacking(data gather.Bytes, contentID ID, comp compression.HeaderID, output *gather.WriteBuffer) (compression.HeaderID, error) {
	var hashOutput [hashing.MaxHashSize]byte

	iv, err := getPackedContentIV(hashOutput[:], contentID)
	if err != nil {
		return NoCompression, errors.Wrapf(err, "unable to get packed content IV for %q", contentID)
	}

	// nolint:nestif
	if comp != NoCompression {
		if sm.format.IndexVersion < v2IndexVersion {
			return NoCompression, errors.Errorf("compression is not enabled for this repository.")
		}

		var tmp gather.WriteBuffer
		defer tmp.Close()

		// allocate temporary buffer to hold the compressed bytes.
		c := compression.ByHeaderID[comp]
		if c == nil {
			return NoCompression, errors.Errorf("unsupported compressor %x", comp)
		}

		if err = c.Compress(&tmp, data.Reader()); err != nil {
			return NoCompression, errors.Wrap(err, "compression error")
		}

		if cd := tmp.Length(); cd >= data.Length() {
			// data was not compressible enough.
			comp = NoCompression
		} else {
			data = tmp.Bytes()
		}
	}

	if err := sm.crypter.Encryptor.Encrypt(data, iv, output); err != nil {
		return NoCompression, errors.Wrap(err, "unable to encrypt")
	}

	sm.Stats.encrypted(data.Length())

	return comp, nil
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

	return errors.Errorf("invalid prefix, must be empty or a single letter between 'g' and 'z'")
}

func (bm *WriteManager) getContentDataReadLocked(ctx context.Context, pp *pendingPackInfo, bi Info, output *gather.WriteBuffer) error {
	var payload gather.WriteBuffer
	defer payload.Close()

	if pp != nil && pp.packBlobID == bi.GetPackBlobID() {
		// we need to use a lock here in case somebody else writes to the pack at the same time.
		if err := pp.currentPackData.AppendSectionTo(&payload, int(bi.GetPackOffset()), int(bi.GetPackedLength())); err != nil {
			// should never happen
			return errors.Wrap(err, "error appending pending content data to buffer")
		}
	} else if err := bm.getCacheForContentID(bi.GetContentID()).getContent(ctx, cacheKey(bi.GetContentID()), bi.GetPackBlobID(), int64(bi.GetPackOffset()), int64(bi.GetPackedLength()), &payload); err != nil {
		return errors.Wrap(err, "error getting cached content")
	}

	return bm.decryptContentAndVerify(payload.Bytes(), bi, output)
}

func (bm *WriteManager) preparePackDataContent(pp *pendingPackInfo) (packIndexBuilder, error) {
	packFileIndex := packIndexBuilder{}
	haveContent := false

	for _, info := range pp.currentPackItems {
		if info.GetPackBlobID() == pp.packBlobID {
			haveContent = true
		}

		bm.log.Debugf("add-to-pack %v %v p:%v %v d:%v", pp.packBlobID, info.GetContentID(), info.GetPackBlobID(), info.GetPackedLength(), info.GetDeleted())

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

	err := bm.appendPackFileIndexRecoveryData(packFileIndex, pp.currentPackData)

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

	return errors.Wrap(bm.st.PutBlob(ctx, packFile, data, blob.PutOptions{}), "error writing pack file")
}

func (sm *SharedManager) hashData(output []byte, data gather.Bytes) []byte {
	// Hash the content and compute encryption key.
	contentID := sm.crypter.HashFunction(output, data)
	sm.Stats.hashedContent(data.Length())

	return contentID
}

// CreateCrypter returns a Crypter based on the specified formatting options.
func CreateCrypter(f *FormattingOptions) (*Crypter, error) {
	h, err := hashing.CreateHashFunc(f)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create hash")
	}

	e, err := encryption.CreateEncryptor(f)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create encryptor")
	}

	contentID := h(nil, gather.FromSlice(nil))

	var tmp gather.WriteBuffer
	defer tmp.Close()

	err = e.Encrypt(gather.FromSlice(nil), contentID, &tmp)
	if err != nil {
		return nil, errors.Wrap(err, "invalid encryptor")
	}

	return &Crypter{h, e}, nil
}
