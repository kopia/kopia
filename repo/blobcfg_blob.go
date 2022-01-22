package repo

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
)

// BlobCfgBlobID is the identifier of a BLOB that describes BLOB retention
// settings for the repository.
const BlobCfgBlobID = "kopia.blobcfg"

type blobCfgBlob struct {
	RetentionMode   string        `json:"retentionMode,omitempty"`
	RetentionPeriod time.Duration `json:"retentionPeriod,omitempty"`
}

func (r *blobCfgBlob) IsRetentionEnabled() bool {
	return r.RetentionMode != "" && r.RetentionPeriod != 0
}

func blobCfgBlobFromOptions(opt *NewRepositoryOptions) *blobCfgBlob {
	return &blobCfgBlob{
		RetentionMode:   opt.RetentionMode,
		RetentionPeriod: opt.RetentionPeriod,
	}
}

func serializeBlobCfgBytes(f *formatBlob, r *blobCfgBlob, masterKey []byte) ([]byte, error) {
	content, err := json.Marshal(r)
	if err != nil {
		return nil, errors.Wrap(err, "can't marshal blobCfgBlob to JSON")
	}

	switch f.EncryptionAlgorithm {
	case "NONE":
		return content, nil

	case aes256GcmEncryption:
		return encryptRepositoryBlobBytesAes256Gcm(content, masterKey, f.UniqueID)

	default:
		return nil, errors.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}
}

func deserializeBlobCfgBytes(f *formatBlob, encryptedBlobCfgBytes, masterKey []byte) (blobCfgBlob, error) {
	var (
		plainText []byte
		r         blobCfgBlob
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
			return blobCfgBlob{}, errors.Errorf("unable to decrypt repository retention blob")
		}

	default:
		return blobCfgBlob{}, errors.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}

	if err = json.Unmarshal(plainText, &r); err != nil {
		return blobCfgBlob{}, errors.Wrap(err, "invalid repository retention blob")
	}

	return r, nil
}
