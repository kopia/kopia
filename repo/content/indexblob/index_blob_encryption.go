package indexblob

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobcrypto"
	"github.com/kopia/kopia/internal/blobparam"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo/blob"
)

// Metadata is an information about a single index blob managed by Manager.
type Metadata struct {
	blob.Metadata
	Superseded []blob.Metadata
}

// WriteValueTo writes the metadata to a JSON writer.
func (m Metadata) WriteValueTo(jw *contentlog.JSONWriter) {
	blobparam.BlobMetadata("metadata", m.Metadata).WriteValueTo(jw)
	jw.BeginListField("superseded")

	for _, bm := range m.Superseded {
		jw.BeginObject()
		jw.StringField("blobID", string(bm.BlobID))
		jw.Int64Field("l", bm.Length)
		jw.TimeField("ts", bm.Timestamp)
		jw.EndObject()
	}
}

type metadataParam struct {
	key string
	val Metadata
}

func (v metadataParam) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(v.key)
	v.val.WriteValueTo(jw)
	jw.EndObject()
}

// MetadataParam creates a parameter for a metadata.
//
//nolint:revive
func MetadataParam(name string, bm Metadata) metadataParam {
	return metadataParam{key: name, val: bm}
}

type metadataListParam struct {
	key  string
	list []Metadata
}

func (v metadataListParam) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginListField(v.key)

	for _, bm := range v.list {
		jw.BeginObject()
		bm.WriteValueTo(jw)
		jw.EndObject()
	}

	jw.EndList()
}

// MetadataListParam creates a parameter for a list of metadata.
//
//nolint:revive
func MetadataListParam(name string, list []Metadata) metadataListParam {
	return metadataListParam{key: name, list: list}
}

// EncryptionManager manages encryption and caching of index blobs.
type EncryptionManager struct {
	st             blob.Storage
	crypter        blobcrypto.Crypter
	indexBlobCache *cache.PersistentCache
	log            *contentlog.Logger
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

	t0 := timetrack.StartTimer()

	bm, err := blob.PutBlobAndGetMetadata(ctx, m.st, blobID, data2.Bytes(), blob.PutOptions{})

	contentlog.Log5(ctx, m.log, "write-index-blob",
		blobparam.BlobID("indexBlobID", blobID),
		logparam.Int("len", data2.Length()),
		logparam.Time("timestamp", bm.Timestamp),
		logparam.Duration("latency", t0.Elapsed()),
		logparam.Error("error", err))

	return bm, errors.Wrapf(err, "error writing blob %v", blobID)
}

// NewEncryptionManager creates new encryption manager.
func NewEncryptionManager(
	st blob.Storage,
	crypter blobcrypto.Crypter,
	indexBlobCache *cache.PersistentCache,
	log *contentlog.Logger,
) *EncryptionManager {
	return &EncryptionManager{st, crypter, indexBlobCache, log}
}
