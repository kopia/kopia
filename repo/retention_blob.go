package repo

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
)

// RetentionBlobID is the identifier of a BLOB that describes BLOB retention
// settings for the repository.
const RetentionBlobID = "kopia.retention"

type retentionBlob struct {
	Mode   string        `json:"mode,omitempty"`
	Period time.Duration `json:"period,omitempty"`
}

func (r *retentionBlob) IsNull() bool {
	return r.Mode == "" || r.Period == 0
}

func retentionBlobFromOptions(opt *NewRepositoryOptions) *retentionBlob {
	return &retentionBlob{
		Mode:   opt.RetentionMode,
		Period: opt.RetentionPeriod,
	}
}

func serializeRetentionBytes(f *formatBlob, r *retentionBlob, masterKey []byte) ([]byte, error) {
	content, err := json.Marshal(r)
	if err != nil {
		return nil, errors.Wrap(err, "can't marshal retentionBlob to JSON")
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

func deserializeRetentionBytes(f *formatBlob, encryptedRetentionBytes, masterKey []byte) (retentionBlob, error) {
	var (
		plainText []byte
		r         retentionBlob
		err       error
	)

	switch f.EncryptionAlgorithm {
	case "NONE": // do nothing
		plainText = encryptedRetentionBytes

	case aes256GcmEncryption:
		plainText, err = decryptRepositoryBlobBytesAes256Gcm(encryptedRetentionBytes, masterKey, f.UniqueID)
		if err != nil {
			return retentionBlob{}, errors.Errorf("unable to decrypt repository retention blob")
		}

	default:
		return retentionBlob{}, errors.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}

	if err = json.Unmarshal(plainText, &r); err != nil {
		return retentionBlob{}, errors.Wrap(err, "invalid repository retention blob")
	}

	return r, nil
}
