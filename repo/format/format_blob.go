// Package format manages kopia.repository and other central format blobs.
package format

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/crypto"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// DefaultFormatEncryption is the identifier of the default format blob encryption algorithm.
const DefaultFormatEncryption = "AES256_GCM"

const (
	aes256GcmEncryption             = "AES256_GCM"
	lengthOfRecoverBlockLength      = 2 // number of bytes used to store recover block length
	maxChecksummedFormatBytesLength = 65000
	maxRecoverChunkLength           = 65536
	minRecoverableChunkLength       = lengthOfRecoverBlockLength + 2
	formatBlobChecksumSize          = sha256.Size
	formatBlobEncryptionKeySize     = 32
)

// KopiaRepositoryBlobID is the identifier of a BLOB that describes repository format.
const KopiaRepositoryBlobID = "kopia.repository"

// ErrInvalidPassword is returned when repository password is invalid.
var ErrInvalidPassword = errors.New("invalid repository password") // +checklocksignore

//nolint:gochecknoglobals
var (
	// formatBlobChecksumSecret is a HMAC secret used for checksumming the format content.
	// It's not really a secret, but will provide positive identification of blocks that
	// are repository format blocks.
	formatBlobChecksumSecret = []byte("kopia-repository")

	errFormatBlobNotFound = errors.New("format blob not found")
)

// KopiaRepositoryJSON represents JSON contents of 'kopia.repository' blob.
type KopiaRepositoryJSON struct {
	Tool         string `json:"tool"`
	BuildVersion string `json:"buildVersion"`
	BuildInfo    string `json:"buildInfo"`

	UniqueID               []byte `json:"uniqueID"`
	KeyDerivationAlgorithm string `json:"keyAlgo"`

	EncryptionAlgorithm string `json:"encryption"`
	// encrypted, serialized JSON encryptedRepositoryConfig{}
	EncryptedFormatBytes []byte `json:"encryptedBlockFormat,omitempty"`
}

// ParseKopiaRepositoryJSON parses the provided byte slice into KopiaRepositoryJSON.
func ParseKopiaRepositoryJSON(b []byte) (*KopiaRepositoryJSON, error) {
	f := &KopiaRepositoryJSON{}

	if err := json.Unmarshal(b, &f); err != nil {
		return nil, errors.Wrap(err, "invalid format blob")
	}

	return f, nil
}

// DeriveFormatEncryptionKeyFromPassword derives encryption key using the provided password and per-repository unique ID.
func (f *KopiaRepositoryJSON) DeriveFormatEncryptionKeyFromPassword(password string) ([]byte, error) {
	res, err := crypto.DeriveKeyFromPassword(password, f.UniqueID, formatBlobEncryptionKeySize, f.KeyDerivationAlgorithm)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to derive format encryption key")
	}

	return res, nil
}

// RecoverFormatBlob attempts to recover format blob replica from the specified file.
// The format blob can be either the prefix or a suffix of the given file.
// optionally the length can be provided (if known) to speed up recovery.
func RecoverFormatBlob(ctx context.Context, st blob.Storage, blobID blob.ID, optionalLength int64) ([]byte, error) {
	if optionalLength > 0 {
		return recoverFormatBlobWithLength(ctx, st, blobID, optionalLength)
	}

	var foundMetadata blob.Metadata

	if err := st.ListBlobs(ctx, blobID, func(bm blob.Metadata) error {
		if foundMetadata.BlobID != "" {
			return errors.Errorf("found multiple blocks with a given prefix: %v", blobID)
		}
		foundMetadata = bm
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "error")
	}

	if foundMetadata.BlobID == "" {
		return nil, blob.ErrBlobNotFound
	}

	return recoverFormatBlobWithLength(ctx, st, foundMetadata.BlobID, foundMetadata.Length)
}

func recoverFormatBlobWithLength(ctx context.Context, st blob.Storage, blobID blob.ID, length int64) ([]byte, error) {
	chunkLength := int64(maxRecoverChunkLength)
	if chunkLength > length {
		chunkLength = length
	}

	if chunkLength <= minRecoverableChunkLength {
		return nil, errFormatBlobNotFound
	}

	// try prefix
	var tmp gather.WriteBuffer
	defer tmp.Close()

	if err := st.GetBlob(ctx, blobID, 0, chunkLength, &tmp); err != nil {
		return nil, errors.Wrapf(err, "error getting blob %v prefix", blobID)
	}

	prefixChunk := tmp.ToByteSlice()

	l := decodeInt16(prefixChunk)
	if l <= maxChecksummedFormatBytesLength && l+lengthOfRecoverBlockLength < len(prefixChunk) {
		if b, ok := verifyFormatBlobChecksum(prefixChunk[lengthOfRecoverBlockLength : lengthOfRecoverBlockLength+l]); ok {
			return b, nil
		}
	}

	// try the suffix
	if err := st.GetBlob(ctx, blobID, length-chunkLength, chunkLength, &tmp); err != nil {
		return nil, errors.Wrapf(err, "error getting blob %v suffix", blobID)
	}

	suffixChunk := tmp.ToByteSlice()

	l = decodeInt16(suffixChunk[len(suffixChunk)-lengthOfRecoverBlockLength:])
	if l <= maxChecksummedFormatBytesLength && l+lengthOfRecoverBlockLength < len(suffixChunk) {
		if b, ok := verifyFormatBlobChecksum(suffixChunk[len(suffixChunk)-lengthOfRecoverBlockLength-l : len(suffixChunk)-lengthOfRecoverBlockLength]); ok {
			return b, nil
		}
	}

	return nil, errFormatBlobNotFound
}

func decodeInt16(b []byte) int {
	return int(b[0]) + int(b[1])<<8
}

func verifyFormatBlobChecksum(b []byte) ([]byte, bool) {
	if len(b) < formatBlobChecksumSize {
		return nil, false
	}

	data, checksum := b[0:len(b)-formatBlobChecksumSize], b[len(b)-formatBlobChecksumSize:]
	h := hmac.New(sha256.New, formatBlobChecksumSecret)
	h.Write(data)
	actualChecksum := h.Sum(nil)

	if !hmac.Equal(actualChecksum, checksum) {
		return nil, false
	}

	return data, true
}

// WriteKopiaRepositoryBlob writes `kopia.repository` blob to a given storage.
func (f *KopiaRepositoryJSON) WriteKopiaRepositoryBlob(ctx context.Context, st blob.Storage, blobCfg BlobStorageConfiguration) error {
	return f.WriteKopiaRepositoryBlobWithID(ctx, st, blobCfg, KopiaRepositoryBlobID)
}

// WriteKopiaRepositoryBlobWithID writes `kopia.repository` blob to a given storage under an alternate blobID.
func (f *KopiaRepositoryJSON) WriteKopiaRepositoryBlobWithID(ctx context.Context, st blob.Storage, blobCfg BlobStorageConfiguration, id blob.ID) error {
	buf := gather.NewWriteBuffer()
	e := json.NewEncoder(buf)
	e.SetIndent("", "  ")

	if err := e.Encode(f); err != nil {
		return errors.Wrap(err, "unable to marshal format blob")
	}

	if err := st.PutBlob(ctx, id, buf.Bytes(), blob.PutOptions{
		RetentionMode:   blobCfg.RetentionMode,
		RetentionPeriod: blobCfg.RetentionPeriod,
	}); err != nil {
		return errors.Wrapf(err, "unable to write format blob %q", id)
	}

	return nil
}

func encryptRepositoryBlobBytesAes256Gcm(data, masterKey, repositoryID []byte) ([]byte, error) {
	res, err := crypto.EncryptAes256Gcm(data, masterKey, repositoryID)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to encrypt blob")
	}

	return res, nil
}

func decryptRepositoryBlobBytesAes256Gcm(data, masterKey, repositoryID []byte) ([]byte, error) {
	res, err := crypto.DecryptAes256Gcm(data, masterKey, repositoryID)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to decrypt blob")
	}

	return res, nil
}

func addFormatBlobChecksumAndLength(fb []byte) ([]byte, error) {
	h := hmac.New(sha256.New, formatBlobChecksumSecret)
	h.Write(fb)
	checksummedFormatBytes := h.Sum(fb)

	l := len(checksummedFormatBytes)
	if l > maxChecksummedFormatBytesLength {
		return nil, errors.Errorf("format blob too big: %v", l)
	}

	// return <length><checksummed-bytes><length>
	result := append([]byte(nil), byte(l), byte(l>>8)) //nolint:mnd
	result = append(result, checksummedFormatBytes...)
	result = append(result, byte(l), byte(l>>8)) //nolint:mnd

	return result, nil
}
