// Package gcs implements Storage based on Google Cloud Storage bucket.
package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/efarrer/iothrottler"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/internal/throttle"
	"github.com/kopia/kopia/repo/blob"

	gcsclient "cloud.google.com/go/storage"
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

func (gcs *gcsStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64) ([]byte, error) {
	if offset < 0 {
		return nil, errors.Errorf("invalid offset")
	}

	attempt := func() (interface{}, error) {
		reader, err := gcs.bucket.Object(gcs.getObjectNameString(b)).NewRangeReader(gcs.ctx, offset, length)
		if err != nil {
			return nil, err
		}
		defer reader.Close() //nolint:errcheck

		return ioutil.ReadAll(reader)
	}

	v, err := exponentialBackoff(ctx, fmt.Sprintf("GetBlob(%q,%v,%v)", b, offset, length), attempt)
	if err != nil {
		return nil, translateError(err)
	}

	fetched := v.([]byte)
	if len(fetched) != int(length) && length >= 0 {
		return nil, errors.Errorf("invalid offset/length")
	}

	return fetched, nil
}

func exponentialBackoff(ctx context.Context, desc string, att retry.AttemptFunc) (interface{}, error) {
	return retry.WithExponentialBackoff(ctx, desc, att, isRetriableError)
}

func isRetriableError(err error) bool {
	if apiError, ok := err.(*googleapi.Error); ok {
		return apiError.Code >= 500
	}

	switch err {
	case nil:
		return false
	case gcsclient.ErrObjectNotExist:
		return false
	case gcsclient.ErrBucketNotExist:
		return false
	default:
		return true
	}
}

func translateError(err error) error {
	switch err {
	case nil:
		return nil
	case gcsclient.ErrObjectNotExist:
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

	combinedLength := data.Length()
	progressCallback := blob.ProgressCallback(ctx)

	if progressCallback != nil {
		progressCallback(string(b), 0, int64(combinedLength))
		defer progressCallback(string(b), int64(combinedLength), int64(combinedLength))

		writer.ProgressFunc = func(completed int64) {
			if completed != int64(combinedLength) {
				progressCallback(string(b), completed, int64(combinedLength))
			}
		}
	}

	_, err := iocopy.Copy(writer, data.Reader())
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

func (gcs *gcsStorage) DeleteBlob(ctx context.Context, b blob.ID) error {
	attempt := func() (interface{}, error) {
		return nil, gcs.bucket.Object(gcs.getObjectNameString(b)).Delete(gcs.ctx)
	}

	_, err := exponentialBackoff(ctx, fmt.Sprintf("DeleteBlob(%q)", b), attempt)
	err = translateError(err)

	if err == blob.ErrBlobNotFound {
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

	if err != iterator.Done {
		return err
	}

	return nil
}

func (gcs *gcsStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   gcsStorageType,
		Config: &gcs.Options,
	}
}

func (gcs *gcsStorage) Close(ctx context.Context) error {
	return gcs.storageClient.Close()
}

func toBandwidth(bytesPerSecond int) iothrottler.Bandwidth {
	if bytesPerSecond <= 0 {
		return iothrottler.Unlimited
	}

	return iothrottler.Bandwidth(bytesPerSecond) * iothrottler.BytesPerSecond
}

func tokenSourceFromCredentialsFile(ctx context.Context, fn string, scopes ...string) (oauth2.TokenSource, error) {
	data, err := ioutil.ReadFile(fn) //nolint:gosec
	if err != nil {
		return nil, err
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
		return nil, err
	}

	downloadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxDownloadSpeedBytesPerSecond))
	uploadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxUploadSpeedBytesPerSecond))

	hc := oauth2.NewClient(ctx, ts)
	hc.Transport = throttle.NewRoundTripper(hc.Transport, downloadThrottler, uploadThrottler)

	cli, err := gcsclient.NewClient(ctx, option.WithHTTPClient(hc))
	if err != nil {
		return nil, err
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
	nonExistentPrefix := fmt.Sprintf("kopia-gcs-storage-initializing-%v", time.Now().UnixNano()) // allow:no-inject-time
	err = gcs.ListBlobs(ctx, blob.ID(nonExistentPrefix), func(md blob.Metadata) error {
		return nil
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to list from the bucket")
	}

	return gcs, nil
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
