// Package gcs implements Storage based on Google Cloud Storage bucket.
package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	gcsclient "cloud.google.com/go/storage"
	"github.com/efarrer/iothrottler"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/throttle"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	gcsStorageType  = "gcs"
	writerChunkSize = 1 << 20
)

type gcsStorage struct {
	Options

	ctx           context.Context
	storageClient *gcsclient.Client
	bucket        *gcsclient.BucketHandle

	downloadThrottler *iothrottler.IOThrottlerPool
	uploadThrottler   *iothrottler.IOThrottlerPool
}

func (gcs *gcsStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	if offset < 0 {
		return blob.ErrInvalidRange
	}

	attempt := func() error {
		reader, err := gcs.bucket.Object(gcs.getObjectNameString(b)).NewRangeReader(gcs.ctx, offset, length)
		if err != nil {
			return errors.Wrap(err, "NewRangeReader")
		}
		defer reader.Close() //nolint:errcheck

		// nolint:wrapcheck
		return iocopy.JustCopy(output, reader)
	}

	if err := attempt(); err != nil {
		return translateError(err)
	}

	// nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func (gcs *gcsStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	attrs, err := gcs.bucket.Object(gcs.getObjectNameString(b)).Attrs(ctx)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(translateError(err), "Attrs")
	}

	return blob.Metadata{
		BlobID:    b,
		Length:    attrs.Size,
		Timestamp: attrs.Created,
	}, nil
}

func translateError(err error) error {
	var ae *googleapi.Error

	if errors.As(err, &ae) {
		if ae.Code == http.StatusRequestedRangeNotSatisfiable {
			return blob.ErrInvalidRange
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

func (gcs *gcsStorage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes) error {
	ctx, cancel := context.WithCancel(ctx)

	obj := gcs.bucket.Object(gcs.getObjectNameString(b))
	writer := obj.NewWriter(ctx)
	writer.ChunkSize = writerChunkSize
	writer.ContentType = "application/x-kopia"

	err := iocopy.JustCopy(writer, data.Reader())
	if err != nil {
		// cancel context before closing the writer causes it to abandon the upload.
		cancel()

		_ = writer.Close() // failing already, ignore the error

		return translateError(err)
	}

	defer cancel()

	// calling close before cancel() causes it to commit the upload.
	return translateError(writer.Close())
}

func (gcs *gcsStorage) SetTime(ctx context.Context, b blob.ID, t time.Time) error {
	return blob.ErrSetTimeUnsupported
}

func (gcs *gcsStorage) DeleteBlob(ctx context.Context, b blob.ID) error {
	err := translateError(gcs.bucket.Object(gcs.getObjectNameString(b)).Delete(gcs.ctx))
	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}

	return err
}

func (gcs *gcsStorage) getObjectNameString(blobID blob.ID) string {
	return gcs.Prefix + string(blobID)
}

func (gcs *gcsStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	lst := gcs.bucket.Objects(gcs.ctx, &gcsclient.Query{
		Prefix: gcs.getObjectNameString(prefix),
	})

	oa, err := lst.Next()
	for err == nil {
		if cberr := callback(blob.Metadata{
			BlobID:    blob.ID(oa.Name[len(gcs.Prefix):]),
			Length:    oa.Size,
			Timestamp: oa.Created,
		}); cberr != nil {
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

func (gcs *gcsStorage) FlushCaches(ctx context.Context) error {
	return nil
}

func toBandwidth(bytesPerSecond int) iothrottler.Bandwidth {
	if bytesPerSecond <= 0 {
		return iothrottler.Unlimited
	}

	return iothrottler.Bandwidth(bytesPerSecond) * iothrottler.BytesPerSecond
}

func tokenSourceFromCredentialsFile(ctx context.Context, fn string, scopes ...string) (oauth2.TokenSource, error) {
	data, err := os.ReadFile(fn)
	if err != nil {
		return nil, errors.Wrap(err, "error reading credentials file")
	}

	cfg, err := google.JWTConfigFromJSON(data, scopes...)
	if err != nil {
		return nil, errors.Wrap(err, "google.JWTConfigFromJSON")
	}

	return cfg.TokenSource(ctx), nil
}

func tokenSourceFromCredentialsJSON(ctx context.Context, data json.RawMessage, scopes ...string) (oauth2.TokenSource, error) {
	cfg, err := google.JWTConfigFromJSON([]byte(data), scopes...)
	if err != nil {
		return nil, errors.Wrap(err, "google.JWTConfigFromJSON")
	}

	return cfg.TokenSource(ctx), nil
}

// New creates new Google Cloud Storage-backed storage with specified options:
//
// - the 'BucketName' field is required and all other parameters are optional.
//
// By default the connection reuses credentials managed by (https://cloud.google.com/sdk/),
// but this can be disabled by setting IgnoreDefaultCredentials to true.
func New(ctx context.Context, opt *Options) (blob.Storage, error) {
	var ts oauth2.TokenSource

	var err error

	scope := gcsclient.ScopeReadWrite
	if opt.ReadOnly {
		scope = gcsclient.ScopeReadOnly
	}

	if sa := opt.ServiceAccountCredentialJSON; len(sa) > 0 {
		ts, err = tokenSourceFromCredentialsJSON(ctx, sa, scope)
	} else if sa := opt.ServiceAccountCredentialsFile; sa != "" {
		ts, err = tokenSourceFromCredentialsFile(ctx, sa, scope)
	} else {
		ts, err = google.DefaultTokenSource(ctx, scope)
	}

	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize token source")
	}

	downloadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxDownloadSpeedBytesPerSecond))
	uploadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxUploadSpeedBytesPerSecond))

	hc := oauth2.NewClient(ctx, ts)
	hc.Transport = throttle.NewRoundTripper(hc.Transport, downloadThrottler, uploadThrottler)

	cli, err := gcsclient.NewClient(ctx, option.WithHTTPClient(hc))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create GCS client")
	}

	if opt.BucketName == "" {
		return nil, errors.New("bucket name must be specified")
	}

	gcs := &gcsStorage{
		Options:           *opt,
		ctx:               ctx,
		storageClient:     cli,
		bucket:            cli.Bucket(opt.BucketName),
		downloadThrottler: downloadThrottler,
		uploadThrottler:   uploadThrottler,
	}

	// verify GCS connection is functional by listing blobs in a bucket, which will fail if the bucket
	// does not exist. We list with a prefix that will not exist, to avoid iterating through any objects.
	nonExistentPrefix := fmt.Sprintf("kopia-gcs-storage-initializing-%v", clock.Now().UnixNano())
	err = gcs.ListBlobs(ctx, blob.ID(nonExistentPrefix), func(md blob.Metadata) error {
		return nil
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to list from the bucket")
	}

	return retrying.NewWrapper(gcs), nil
}

func init() {
	blob.AddSupportedStorage(
		gcsStorageType,
		func() interface{} {
			return &Options{}
		},
		func(ctx context.Context, o interface{}) (blob.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
