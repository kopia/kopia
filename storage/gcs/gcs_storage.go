// Package gcs implements Storage based on Google Cloud Storage bucket.
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/internal/throttle"

	"golang.org/x/oauth2/google"

	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/efarrer/iothrottler"
	"github.com/kopia/kopia/storage"
	"golang.org/x/oauth2"

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

func (gcs *gcsStorage) GetBlock(b string, offset, length int64) ([]byte, error) {
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

	return v.([]byte), nil
}

func exponentialBackoff(desc string, att retry.AttemptFunc) (interface{}, error) {
	return retry.WithExponentialBackoff(desc, att, isRetriableError)
}

func isRetriableError(err error) bool {
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

func (gcs *gcsStorage) PutBlock(b string, data []byte) error {
	attempt := func() (interface{}, error) {
		ctx, cancel := context.WithCancel(gcs.ctx)

		writer := gcs.bucket.Object(gcs.getObjectNameString(b)).NewWriter(ctx)
		n, err := writer.Write(data)
		if err != nil {
			cancel()
			return nil, err
		}
		if n != len(data) {
			cancel()
			return nil, fmt.Errorf("truncated write %v of %v bytes", n, len(data))
		}

		defer cancel()
		return nil, writer.Close()
	}

	_, err := exponentialBackoff(fmt.Sprintf("PutBlock(%q)", b), attempt)
	return translateError(err)
}

func (gcs *gcsStorage) DeleteBlock(b string) error {
	attempt := func() (interface{}, error) {
		return nil, gcs.bucket.Object(gcs.getObjectNameString(b)).Delete(gcs.ctx)
	}

	_, err := exponentialBackoff(fmt.Sprintf("DeleteBlock(%q)", b), attempt)
	return translateError(err)
}

func (gcs *gcsStorage) getObjectNameString(blockID string) string {
	return gcs.Prefix + blockID
}

func (gcs *gcsStorage) ListBlocks(prefix string) (<-chan storage.BlockMetadata, storage.CancelFunc) {
	ch := make(chan storage.BlockMetadata, 100)
	cancelled := make(chan bool)

	go func() {
		defer close(ch)

		lst := gcs.bucket.Objects(gcs.ctx, &gcsclient.Query{
			Prefix: gcs.getObjectNameString(prefix),
		})

		oa, err := lst.Next()
		for err == nil {
			bm := storage.BlockMetadata{
				BlockID:   oa.Name[len(gcs.Prefix):],
				Length:    oa.Size,
				TimeStamp: oa.Created,
			}
			select {
			case ch <- bm:
			case <-cancelled:
				return
			}
			oa, err = lst.Next()
		}

		if err != iterator.Done {
			select {
			case ch <- storage.BlockMetadata{Error: translateError(err)}:
				return
			case <-cancelled:
				return
			}
		}
	}()

	return ch, func() {
		close(cancelled)
	}
}

func (gcs *gcsStorage) ConnectionInfo() storage.ConnectionInfo {
	return storage.ConnectionInfo{
		Type:   gcsStorageType,
		Config: &gcs.Options,
	}
}

func (gcs *gcsStorage) Close() error {
	gcs.storageClient.Close() //nolint:errcheck
	return nil
}

func (gcs *gcsStorage) String() string {
	return fmt.Sprintf("gcs://%v/%v", gcs.BucketName, gcs.Prefix)
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
