// Package gcs implements Storage based on Google Cloud Storage bucket.
package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"google.golang.org/api/googleapi"

	"github.com/efarrer/iothrottler"
	"github.com/kopia/repo/internal/retry"
	"github.com/kopia/repo/internal/throttle"
	"github.com/kopia/repo/storage"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	gcsclient "cloud.google.com/go/storage"
)

const (
	gcsStorageType = "gcs"
)

type gcsStorage struct {
	Options

	ctx           context.Context
	storageClient *gcsclient.Client
	bucket        *gcsclient.BucketHandle

	downloadThrottler *iothrottler.IOThrottlerPool
	uploadThrottler   *iothrottler.IOThrottlerPool
}

func (gcs *gcsStorage) GetBlock(ctx context.Context, b string, offset, length int64) ([]byte, error) {
	if offset < 0 {
		return nil, fmt.Errorf("invalid offset")
	}

	attempt := func() (interface{}, error) {
		reader, err := gcs.bucket.Object(gcs.getObjectNameString(b)).NewRangeReader(gcs.ctx, offset, length)
		if err != nil {
			return nil, err
		}
		defer reader.Close() //nolint:errcheck

		return ioutil.ReadAll(reader)
	}

	v, err := exponentialBackoff(fmt.Sprintf("GetBlock(%q,%v,%v)", b, offset, length), attempt)
	if err != nil {
		return nil, translateError(err)
	}

	fetched := v.([]byte)
	if len(fetched) != int(length) && length >= 0 {
		return nil, fmt.Errorf("invalid offset/length")
	}

	return fetched, nil
}

func exponentialBackoff(desc string, att retry.AttemptFunc) (interface{}, error) {
	return retry.WithExponentialBackoff(desc, att, isRetriableError)
}

func isRetriableError(err error) bool {
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code >= 500 {
			return true
		}
		return false
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
		return storage.ErrBlockNotFound
	case gcsclient.ErrBucketNotExist:
		return storage.ErrBlockNotFound
	default:
		return fmt.Errorf("unexpected GCS error: %v", err)
	}
}
func (gcs *gcsStorage) PutBlock(ctx context.Context, b string, data []byte) error {
	ctx, cancel := context.WithCancel(ctx)

	obj := gcs.bucket.Object(gcs.getObjectNameString(b))
	writer := obj.NewWriter(ctx)
	writer.ChunkSize = 1 << 20
	writer.ContentType = "application/x-kopia"

	progressCallback := storage.ProgressCallback(ctx)

	if progressCallback != nil {
		progressCallback(b, 0, int64(len(data)))
		defer progressCallback(b, int64(len(data)), int64(len(data)))

		writer.ProgressFunc = func(completed int64) {
			if completed != int64(len(data)) {
				progressCallback(b, completed, int64(len(data)))
			}
		}
	}

	_, err := io.Copy(writer, bytes.NewReader(data))
	if err != nil {
		// cancel context before closing the writer causes it to abandon the upload.
		cancel()
		writer.Close() //nolint:errcheck
		return translateError(err)
	}
	defer cancel()

	// calling close before cancel() causes it to commit the upload.
	return translateError(writer.Close())
}

func (gcs *gcsStorage) DeleteBlock(ctx context.Context, b string) error {
	attempt := func() (interface{}, error) {
		return nil, gcs.bucket.Object(gcs.getObjectNameString(b)).Delete(gcs.ctx)
	}

	_, err := exponentialBackoff(fmt.Sprintf("DeleteBlock(%q)", b), attempt)
	err = translateError(err)
	if err == storage.ErrBlockNotFound {
		return nil
	}

	return err
}

func (gcs *gcsStorage) getObjectNameString(blockID string) string {
	return gcs.Prefix + blockID
}

func (gcs *gcsStorage) ListBlocks(ctx context.Context, prefix string, callback func(storage.BlockMetadata) error) error {
	lst := gcs.bucket.Objects(gcs.ctx, &gcsclient.Query{
		Prefix: gcs.getObjectNameString(prefix),
	})

	oa, err := lst.Next()
	for err == nil {
		if err = callback(storage.BlockMetadata{
			BlockID:   oa.Name[len(gcs.Prefix):],
			Length:    oa.Size,
			Timestamp: oa.Created,
		}); err != nil {
			return err
		}
		oa, err = lst.Next()
	}

	if err != iterator.Done {
		return err
	}

	return nil
}

func (gcs *gcsStorage) ConnectionInfo() storage.ConnectionInfo {
	return storage.ConnectionInfo{
		Type:   gcsStorageType,
		Config: &gcs.Options,
	}
}

func (gcs *gcsStorage) Close(ctx context.Context) error {
	gcs.storageClient.Close() //nolint:errcheck
	return nil
}

func toBandwidth(bytesPerSecond int) iothrottler.Bandwidth {
	if bytesPerSecond <= 0 {
		return iothrottler.Unlimited
	}

	return iothrottler.Bandwidth(bytesPerSecond) * iothrottler.BytesPerSecond
}

func tokenSourceFromCredentialsFile(ctx context.Context, fn string, scopes ...string) (oauth2.TokenSource, error) {
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		return nil, err
	}

	cfg, err := google.JWTConfigFromJSON(data, scopes...)
	if err != nil {
		return nil, fmt.Errorf("google.JWTConfigFromJSON: %v", err)
	}
	return cfg.TokenSource(ctx), nil
}

// New creates new Google Cloud Storage-backed storage with specified options:
//
// - the 'BucketName' field is required and all other parameters are optional.
//
// By default the connection reuses credentials managed by (https://cloud.google.com/sdk/),
// but this can be disabled by setting IgnoreDefaultCredentials to true.
func New(ctx context.Context, opt *Options) (storage.Storage, error) {
	var ts oauth2.TokenSource
	var err error

	scope := gcsclient.ScopeReadWrite
	if opt.ReadOnly {
		scope = gcsclient.ScopeReadOnly
	}

	if sa := opt.ServiceAccountCredentials; sa != "" {
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

	return &gcsStorage{
		Options:           *opt,
		ctx:               ctx,
		storageClient:     cli,
		bucket:            cli.Bucket(opt.BucketName),
		downloadThrottler: downloadThrottler,
		uploadThrottler:   uploadThrottler,
	}, nil
}

func init() {
	storage.AddSupportedStorage(
		gcsStorageType,
		func() interface{} {
			return &Options{}
		},
		func(ctx context.Context, o interface{}) (storage.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
