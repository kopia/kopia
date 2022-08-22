package content

import (
	"context"
	"crypto/aes"
	cryptorand "crypto/rand"
	"fmt"
	"io"
	"strings"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/logging"
)

const indexBlobCompactionWarningThreshold = 1000

func (sm *SharedManager) maybeCompressAndEncryptDataForPacking(data gather.Bytes, contentID ID, comp compression.HeaderID, output *gather.WriteBuffer, mp format.MutableParameters) (compression.HeaderID, error) {
	var hashOutput [hashing.MaxHashSize]byte

	iv := getPackedContentIV(hashOutput[:0], contentID)

	// If the content is prefixed (which represents Kopia's own metadata as opposed to user data),
	// and we're on V2 format or greater, enable internal compression even when not requested.
	if contentID.HasPrefix() && comp == NoCompression && mp.IndexVersion >= index.Version2 {
		// 'zstd-fastest' has a good mix of being fast, low memory usage and high compression for JSON.
		comp = compression.HeaderZstdFastest
	}

	//nolint:nestif
	if comp != NoCompression {
		if mp.IndexVersion < index.Version2 {
			return NoCompression, errors.Errorf("compression is not enabled for this repository")
		}

		var tmp gather.WriteBuffer
		defer tmp.Close()

		// allocate temporary buffer to hold the compressed bytes.
		c := compression.ByHeaderID[comp]
		if c == nil {
			return NoCompression, errors.Errorf("unsupported compressor %x", comp)
		}

		if err := c.Compress(&tmp, data.Reader()); err != nil {
			return NoCompression, errors.Wrap(err, "compression error")
		}

		if cd := tmp.Length(); cd >= data.Length() {
			// data was not compressible enough.
			comp = NoCompression
		} else {
			data = tmp.Bytes()
		}
	}

	if err := sm.format.Encryptor().Encrypt(data, iv, output); err != nil {
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

func contentCacheKeyForInfo(bi Info) string {
	// append format-specific information
	// see https://github.com/kopia/kopia/issues/1843 for an explanation
	return fmt.Sprintf("%v.%x.%x.%x", bi.GetContentID(), bi.GetCompressionHeaderID(), bi.GetFormatVersion(), bi.GetEncryptionKeyID())
}

func (sm *SharedManager) getContentDataReadLocked(ctx context.Context, pp *pendingPackInfo, bi Info, output *gather.WriteBuffer) error {
	var payload gather.WriteBuffer
	defer payload.Close()

	if pp != nil && pp.packBlobID == bi.GetPackBlobID() {
		// we need to use a lock here in case somebody else writes to the pack at the same time.
		if err := pp.currentPackData.AppendSectionTo(&payload, int(bi.GetPackOffset()), int(bi.GetPackedLength())); err != nil {
			// should never happen
			return errors.Wrap(err, "error appending pending content data to buffer")
		}
	} else if err := sm.getCacheForContentID(bi.GetContentID()).GetContent(ctx, contentCacheKeyForInfo(bi), bi.GetPackBlobID(), int64(bi.GetPackOffset()), int64(bi.GetPackedLength()), &payload); err != nil {
		return errors.Wrap(err, "error getting cached content")
	}

	info := encryption.DecryptInfo{}

	err := sm.decryptContentAndVerify(payload.Bytes(), bi, output, &info)
	if err != nil {
		return err
	}

	if info.CorrectedBlocksByECC > 0 {
		// TODO Write corrected blob
	}

	return nil
}

func (sm *SharedManager) preparePackDataContent(pp *pendingPackInfo) (index.Builder, error) {
	packFileIndex := index.Builder{}
	haveContent := false

	sb := logging.GetBuffer()
	defer sb.Release()

	for _, info := range pp.currentPackItems {
		if info.GetPackBlobID() == pp.packBlobID {
			haveContent = true
		}

		sb.Reset()
		sb.AppendString("add-to-pack ")
		sb.AppendString(string(pp.packBlobID))
		sb.AppendString(" ")
		info.GetContentID().AppendToLogBuffer(sb)
		sb.AppendString(" p:")
		sb.AppendString(string(info.GetPackBlobID()))
		sb.AppendString(" ")
		sb.AppendUint32(info.GetPackedLength())
		sb.AppendString(" d:")
		sb.AppendBoolean(info.GetDeleted())
		sm.log.Debugf(sb.String())

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

	if sm.paddingUnit > 0 {
		if missing := sm.paddingUnit - (pp.currentPackData.Length() % sm.paddingUnit); missing > 0 {
			if err := writeRandomBytesToBuffer(pp.currentPackData, missing); err != nil {
				return nil, errors.Wrap(err, "unable to prepare content postamble")
			}
		}
	}

	err := sm.appendPackFileIndexRecoveryData(packFileIndex, pp.currentPackData)

	return packFileIndex, err
}

func getPackedContentIV(output []byte, contentID ID) []byte {
	h := contentID.Hash()

	return append(output, h[len(h)-aes.BlockSize:]...)
}

func (sm *SharedManager) writePackFileNotLocked(ctx context.Context, packFile blob.ID, data gather.Bytes, onUpload func(int64)) error {
	ctx, span := tracer.Start(ctx, "WritePackFile_"+strings.ToUpper(string(packFile[0:1])), trace.WithAttributes(attribute.String("packFile", string(packFile))))
	defer span.End()

	sm.Stats.wroteContent(data.Length())
	onUpload(int64(data.Length()))

	return errors.Wrap(sm.st.PutBlob(ctx, packFile, data, blob.PutOptions{}), "error writing pack file")
}

func (sm *SharedManager) hashData(output []byte, data gather.Bytes) []byte {
	// Hash the content and compute encryption key.
	contentID := sm.format.HashFunc()(output, data)
	sm.Stats.hashedContent(data.Length())

	return contentID
}
