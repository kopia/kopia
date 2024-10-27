package format

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// KopiaBlobCfgBlobID is the identifier of a BLOB that describes BLOB retention
// settings for the repository.
const KopiaBlobCfgBlobID = "kopia.blobcfg"

// BlobStorageConfiguration is the content for `kopia.blobcfg` blob which contains the blob
// storage configuration options.
type BlobStorageConfiguration struct {
	RetentionMode   blob.RetentionMode `json:"retentionMode,omitempty"`
	RetentionPeriod time.Duration      `json:"retentionPeriod,omitempty"`
}

// IsRetentionEnabled returns true if retention is enabled on the blob-config
// object.
func (r *BlobStorageConfiguration) IsRetentionEnabled() bool {
	return r.RetentionMode != "" && r.RetentionPeriod != 0
}

// Validate validates the blob config parameters.
func (r *BlobStorageConfiguration) Validate() error {
	if (r.RetentionMode == "") != (r.RetentionPeriod == 0) {
		return errors.New("both retention mode and period must be provided when setting blob retention properties")
	}

	if r.RetentionPeriod != 0 && r.RetentionPeriod < 24*time.Hour {
		return errors.New("invalid retention-period, the minimum required is 1-day and there is no maximum limit")
	}

	return nil
}

func serializeBlobCfgBytes(f *KopiaRepositoryJSON, r BlobStorageConfiguration, formatEncryptionKey []byte) ([]byte, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return nil, errors.Wrap(err, "can't marshal blobCfgBlob to JSON")
	}

	switch f.EncryptionAlgorithm {
	case "NONE":
		return data, nil

	case aes256GcmEncryption:
		return encryptRepositoryBlobBytesAes256Gcm(data, formatEncryptionKey, f.UniqueID)

	default:
		return nil, errors.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}
}

// deserializeBlobCfgBytes decrypts and deserializes the given bytes into BlobStorageConfiguration.
func deserializeBlobCfgBytes(j *KopiaRepositoryJSON, encryptedBlobCfgBytes, formatEncryptionKey []byte) (BlobStorageConfiguration, error) {
	var (
		plainText []byte
		r         BlobStorageConfiguration
		err       error
	)

	if encryptedBlobCfgBytes == nil {
		return r, nil
	}

	switch j.EncryptionAlgorithm {
	case "NONE": // do nothing
		plainText = encryptedBlobCfgBytes

	case aes256GcmEncryption:
		plainText, err = decryptRepositoryBlobBytesAes256Gcm(encryptedBlobCfgBytes, formatEncryptionKey, j.UniqueID)
		if err != nil {
			return BlobStorageConfiguration{}, errors.New("unable to decrypt repository blobcfg blob")
		}

	default:
		return BlobStorageConfiguration{}, errors.Errorf("unknown encryption algorithm: '%v'", j.EncryptionAlgorithm)
	}

	if err = json.Unmarshal(plainText, &r); err != nil {
		return BlobStorageConfiguration{}, errors.Wrap(err, "invalid repository blobcfg blob")
	}

	return r, nil
}

// WriteBlobCfgBlob writes `kopia.blobcfg` encrypted using the provided key.
func (f *KopiaRepositoryJSON) WriteBlobCfgBlob(ctx context.Context, st blob.Storage, blobcfg BlobStorageConfiguration, formatEncryptionKey []byte) error {
	blobCfgBytes, err := serializeBlobCfgBytes(f, blobcfg, formatEncryptionKey)
	if err != nil {
		return errors.Wrap(err, "unable to encrypt blobcfg bytes")
	}

	if err := st.PutBlob(ctx, KopiaBlobCfgBlobID, gather.FromSlice(blobCfgBytes), blob.PutOptions{
		RetentionMode:   blobcfg.RetentionMode,
		RetentionPeriod: blobcfg.RetentionPeriod,
	}); err != nil {
		return errors.Wrapf(err, "PutBlob() failed for %q", KopiaBlobCfgBlobID)
	}

	return nil
}
