package indexblob

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobcrypto"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

// Metadata is an information about a single index blob managed by Manager.
type Metadata struct {
	blob.Metadata
	Superseded []blob.Metadata
}

// EncryptionManager manages encryption and caching of index blobs.
type EncryptionManager struct {
	st             blob.Storage
	crypter        blobcrypto.Crypter
	indexBlobCache *cache.PersistentCache
	log            logging.Logger
}

// GetEncryptedBlob fetches and decrypts the contents of a given encrypted blob
// using cache first and falling back to the underlying storage.
func (m *EncryptionManager) GetEncryptedBlob(ctx context.Context, blobID blob.ID, output *gather.WriteBuffer) error {
	var payload gather.WriteBuffer
	defer payload.Close()

	if err := m.indexBlobCache.GetOrLoad(ctx, string(blobID), func(output *gather.WriteBuffer) error {
		return m.st.GetBlob(ctx, blobID, 0, -1, output)
	}, &payload); err != nil {
		return errors.Wrap(err, "getContent")
	}

	return errors.Wrap(blobcrypto.Decrypt(m.crypter, payload.Bytes(), blobID, output), "decrypt blob")
}

// EncryptAndWriteBlob encrypts and writes the provided data into a blob,
// with name {prefix}{hash}[-{suffix}].
func (m *EncryptionManager) EncryptAndWriteBlob(ctx context.Context, data gather.Bytes, prefix, suffix blob.ID) (blob.Metadata, error) {
	var data2 gather.WriteBuffer
	defer data2.Close()

	blobID, err := blobcrypto.Encrypt(m.crypter, data, prefix, suffix, &data2)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(err, "error encrypting")
	}

	bm, err := blob.PutBlobAndGetMetadata(ctx, m.st, blobID, data2.Bytes(), blob.PutOptions{})
	if err != nil {
		m.log.Debugf("write-index-blob %v failed %v", blobID, err)
		return blob.Metadata{}, errors.Wrapf(err, "error writing blob %v", blobID)
	}

	m.log.Debugf("write-index-blob %v %v %v", blobID, bm.Length, bm.Timestamp)

	return bm, nil
}

// NewEncryptionManager creates new encryption manager.
func NewEncryptionManager(
	st blob.Storage,
	crypter blobcrypto.Crypter,
	indexBlobCache *cache.PersistentCache,
	log logging.Logger,
) *EncryptionManager {
	return &EncryptionManager{st, crypter, indexBlobCache, log}
}
