package repo

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

// BlobCfgBlobID is the identifier of a BLOB that describes BLOB retention
// settings for the repository.
const BlobCfgBlobID = "kopia.blobcfg"

func blobCfgBlobFromOptions(opt *NewRepositoryOptions) content.BlobCfgBlob {
	return content.BlobCfgBlob{
		RetentionMode:   opt.RetentionMode,
		RetentionPeriod: opt.RetentionPeriod,
	}
}

func serializeBlobCfgBytes(f *formatBlob, r content.BlobCfgBlob, masterKey []byte) ([]byte, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return nil, errors.Wrap(err, "can't marshal blobCfgBlob to JSON")
	}

	switch f.EncryptionAlgorithm {
	case "NONE":
		return data, nil

	case aes256GcmEncryption:
		return encryptRepositoryBlobBytesAes256Gcm(data, masterKey, f.UniqueID)

	default:
		return nil, errors.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}
}

func deserializeBlobCfgBytes(f *formatBlob, encryptedBlobCfgBytes, masterKey []byte) (content.BlobCfgBlob, error) {
	var (
		plainText []byte
		r         content.BlobCfgBlob
		err       error
	)

	if encryptedBlobCfgBytes == nil {
		return r, nil
	}

	switch f.EncryptionAlgorithm {
	case "NONE": // do nothing
		plainText = encryptedBlobCfgBytes

	case aes256GcmEncryption:
		plainText, err = decryptRepositoryBlobBytesAes256Gcm(encryptedBlobCfgBytes, masterKey, f.UniqueID)
		if err != nil {
			return content.BlobCfgBlob{}, errors.Errorf("unable to decrypt repository blobcfg blob")
		}

	default:
		return content.BlobCfgBlob{}, errors.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}

	if err = json.Unmarshal(plainText, &r); err != nil {
		return content.BlobCfgBlob{}, errors.Wrap(err, "invalid repository blobcfg blob")
	}

	return r, nil
}

func writeBlobCfgBlob(ctx context.Context, st blob.Storage, f *formatBlob, blobcfg content.BlobCfgBlob, formatEncryptionKey []byte) error {
	blobCfgBytes, err := serializeBlobCfgBytes(f, blobcfg, formatEncryptionKey)
	if err != nil {
		return errors.Wrap(err, "unable to encrypt blobcfg bytes")
	}

	if err := st.PutBlob(ctx, BlobCfgBlobID, gather.FromSlice(blobCfgBytes), blob.PutOptions{
		RetentionMode:   blobcfg.RetentionMode,
		RetentionPeriod: blobcfg.RetentionPeriod,
	}); err != nil {
		return errors.Wrapf(err, "PutBlob() failed for %q", BlobCfgBlobID)
	}

	return nil
}
