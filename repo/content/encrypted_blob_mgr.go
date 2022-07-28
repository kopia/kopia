package content

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

type encryptedBlobMgr struct {
	st              blob.Storage
	crypterProvider CrypterProvider
	indexBlobCache  *cache.PersistentCache
	log             logging.Logger
}

func (m *encryptedBlobMgr) getEncryptedBlob(ctx context.Context, blobID blob.ID, output *gather.WriteBuffer) error {
	var payload gather.WriteBuffer
	defer payload.Close()

	if err := m.indexBlobCache.GetOrLoad(ctx, string(blobID), func(output *gather.WriteBuffer) error {
		// nolint:wrapcheck
		return m.st.GetBlob(ctx, blobID, 0, -1, output)
	}, &payload); err != nil {
		return errors.Wrap(err, "getContent")
	}

	return m.crypterProvider.Crypter().DecryptBLOB(payload.Bytes(), blobID, output)
}

func (m *encryptedBlobMgr) encryptAndWriteBlob(ctx context.Context, data gather.Bytes, prefix blob.ID, sessionID SessionID) (blob.Metadata, error) {
	var data2 gather.WriteBuffer
	defer data2.Close()

	blobID, err := m.crypterProvider.Crypter().EncryptBLOB(data, prefix, sessionID, &data2)
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
