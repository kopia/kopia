// Package gcs implements Storage based on Google Cloud Storage bucket.
package gcs

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	gcsclient "cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/timestampmeta"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	gcsStorageType  = "gcs"
	writerChunkSize = 1 << 20
	latestVersionID = ""

	timeMapKey = "Kopia-Mtime" // case is important, first letter must be capitalized.
)

type gcsStorage struct {
	Options
	blob.DefaultProviderImplementation

	storageClient *gcsclient.Client
	bucket        *gcsclient.BucketHandle
}

func (gcs *gcsStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	return gcs.getBlobWithVersion(ctx, b, latestVersionID, offset, length, output)
}

// getBlobWithVersion returns full or partial contents of a blob with given ID and version.
func (gcs *gcsStorage) getBlobWithVersion(ctx context.Context, b blob.ID, version string, offset, length int64, output blob.OutputBuffer) error {
	if offset < 0 {
		return blob.ErrInvalidRange
	}

	obj := gcs.bucket.Object(gcs.getObjectNameString(b))

	if version != "" {
		gen, err := strconv.ParseInt(version, 10, 64)
		if err != nil {
			return errors.Wrap(err, "failed to parse blob version")
		}

		obj = obj.Generation(gen)
	}

	attempt := func() error {
		reader, err := obj.NewRangeReader(ctx, offset, length)
		if err != nil {
			return errors.Wrap(err, "NewRangeReader")
		}
		defer reader.Close() //nolint:errcheck

		return iocopy.JustCopy(output, reader)
	}

	if err := attempt(); err != nil {
		return translateError(err)
	}

	//nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func (gcs *gcsStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	objName := gcs.getObjectNameString(b)
	obj := gcs.bucket.Object(objName)

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(translateError(err), "Attrs")
	}

	return gcs.getBlobMeta(attrs), nil
}

func (gcs *gcsStorage) getBlobMeta(attrs *gcsclient.ObjectAttrs) blob.Metadata {
	bm := blob.Metadata{
		BlobID:    gcs.toBlobID(attrs.Name),
		Length:    attrs.Size,
		Timestamp: attrs.Created,
	}

	if t, ok := timestampmeta.FromValue(attrs.Metadata[timeMapKey]); ok {
		bm.Timestamp = t
	}

	return bm
}

func translateError(err error) error {
	var ae *googleapi.Error

	if errors.As(err, &ae) {
		switch ae.Code {
		case http.StatusRequestedRangeNotSatisfiable:
			return blob.ErrInvalidRange
		case http.StatusPreconditionFailed:
			return blob.ErrBlobAlreadyExists
		}
	}

	switch {
	case err == nil:
		return nil
	case errors.Is(err, gcsclient.ErrObjectNotExist):
		return blob.ErrBlobNotFound
	default:
		return errors.Wrap(err, "unexpected GCS error")
	}
}

func (gcs *gcsStorage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	ctx, cancel := context.WithCancel(ctx)

	obj := gcs.bucket.Object(gcs.getObjectNameString(b))

	conds := gcsclient.Conditions{DoesNotExist: opts.DoNotRecreate}
	if conds != (gcsclient.Conditions{}) {
		obj = obj.If(conds)
	}

	writer := obj.NewWriter(ctx)
	writer.ChunkSize = writerChunkSize
	writer.ContentType = "application/x-kopia"
	writer.ObjectAttrs.Metadata = timestampmeta.ToMap(opts.SetModTime, timeMapKey)

	if opts.RetentionPeriod != 0 {
		retainUntilDate := clock.Now().Add(opts.RetentionPeriod).UTC()
		writer.ObjectAttrs.Retention = &gcsclient.ObjectRetention{
			Mode:        string(blob.Locked),
			RetainUntil: retainUntilDate,
		}
	}

	err := iocopy.JustCopy(writer, data.Reader())
	if err != nil {
		// cancel context before closing the writer causes it to abandon the upload.
		cancel()

		_ = writer.Close() // failing already, ignore the error

		return translateError(err)
	}

	defer cancel()

	// calling close before cancel() causes it to commit the upload.
	if err := writer.Close(); err != nil {
		return translateError(err)
	}

	if opts.GetModTime != nil {
		*opts.GetModTime = writer.Attrs().Updated
	}

	return nil
}

func (gcs *gcsStorage) DeleteBlob(ctx context.Context, b blob.ID) error {
	err := translateError(gcs.bucket.Object(gcs.getObjectNameString(b)).Delete(ctx))
	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}

	return err
}

func (gcs *gcsStorage) ExtendBlobRetention(ctx context.Context, b blob.ID, opts blob.ExtendOptions) error {
	retainUntilDate := clock.Now().Add(opts.RetentionPeriod).UTC().Truncate(time.Second)

	r := &gcsclient.ObjectRetention{
		Mode:        string(blob.Locked),
		RetainUntil: retainUntilDate,
	}

	_, err := gcs.bucket.Object(gcs.getObjectNameString(b)).Update(ctx, gcsclient.ObjectAttrsToUpdate{Retention: r})
	if err != nil {
		return errors.Wrap(err, "unable to extend retention period to "+retainUntilDate.String())
	}

	return nil
}

func (gcs *gcsStorage) getObjectNameString(blobID blob.ID) string {
	return gcs.Prefix + string(blobID)
}

func (gcs *gcsStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	lst := gcs.bucket.Objects(ctx, &gcsclient.Query{
		Prefix: gcs.getObjectNameString(prefix),
	})

	oa, err := lst.Next()
	for err == nil {
		bm := gcs.getBlobMeta(oa)

		if cberr := callback(bm); cberr != nil {
			return cberr
		}

		oa, err = lst.Next()
	}

	if !errors.Is(err, iterator.Done) {
		return errors.Wrap(err, "ListBlobs")
	}

	return nil
}

func (gcs *gcsStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   gcsStorageType,
		Config: &gcs.Options,
	}
}

func (gcs *gcsStorage) DisplayName() string {
	return fmt.Sprintf("GCS: %v", gcs.BucketName)
}

func (gcs *gcsStorage) Close(ctx context.Context) error {
	return errors.Wrap(gcs.storageClient.Close(), "error closing GCS storage")
}

func (gcs *gcsStorage) toBlobID(blobName string) blob.ID {
	return blob.ID(blobName[len(gcs.Prefix):])
}

// New creates new Google Cloud Storage-backed storage with specified options:
//
// - the 'BucketName' field is required and all other parameters are optional.
//
// By default the connection reuses credentials managed by (https://cloud.google.com/sdk/),
// but this can be disabled by setting IgnoreDefaultCredentials to true.
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
	_ = isCreate

	if opt.BucketName == "" {
		return nil, errors.New("bucket name must be specified")
	}

	scope := gcsclient.ScopeFullControl
	if opt.ReadOnly {
		scope = gcsclient.ScopeReadOnly
	}

	clientOptions := []option.ClientOption{option.WithScopes(scope)}

	if j := opt.ServiceAccountCredentialJSON; len(j) > 0 {
		clientOptions = append(clientOptions, option.WithCredentialsJSON(j))
	} else if fn := opt.ServiceAccountCredentialsFile; fn != "" {
		clientOptions = append(clientOptions, option.WithCredentialsFile(fn))
	}

	cli, err := gcsclient.NewClient(ctx, clientOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create GCS client")
	}

	st := &gcsStorage{
		Options:       *opt,
		storageClient: cli,
		bucket:        cli.Bucket(opt.BucketName),
	}

	gcs, err := maybePointInTimeStore(ctx, st, opt.PointInTime)
	if err != nil {
		return nil, err
	}

	// verify GCS connection is functional by listing blobs in a bucket, which will fail if the bucket
	// does not exist. We list with a prefix that will not exist, to avoid iterating through any objects.
	nonExistentPrefix := fmt.Sprintf("kopia-gcs-storage-initializing-%v", clock.Now().UnixNano())

	err = gcs.ListBlobs(ctx, blob.ID(nonExistentPrefix), func(_ blob.Metadata) error {
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to list from the bucket")
	}

	return retrying.NewWrapper(gcs), nil
}

func init() {
	blob.AddSupportedStorage(gcsStorageType, Options{}, New)
}
