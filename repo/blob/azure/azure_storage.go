// Package azure implements Azure Blob Storage.
package azure

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/efarrer/iothrottler"
	"github.com/pkg/errors"
	gblob "gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
	"gocloud.dev/gcerrors"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
)

const (
	azStorageType = "azureBlob"
)

type azStorage struct {
	Options

	ctx context.Context

	bucket *gblob.Bucket

	downloadThrottler *iothrottler.IOThrottlerPool
	uploadThrottler   *iothrottler.IOThrottlerPool
}

func (az *azStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64) ([]byte, error) {
	if offset < 0 {
		return nil, errors.Errorf("invalid offset")
	}

	attempt := func() (interface{}, error) {
		reader, err := az.bucket.NewRangeReader(ctx, az.getObjectNameString(b), offset, length, nil)
		if err != nil {
			return nil, err
		}

		defer reader.Close() //nolint:errcheck

		throttled, err := az.downloadThrottler.AddReader(reader)
		if err != nil {
			return nil, err
		}

		return ioutil.ReadAll(throttled)
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
	if me, ok := err.(azblob.ResponseError); ok {
		if me.Response() == nil {
			return true
		}
		// retry on server errors, not on client errors
		return me.Response().StatusCode >= 500
	}

	// https://pkg.go.dev/gocloud.dev/gcerrors?tab=doc#ErrorCode
	switch gcerrors.Code(err) {
	case gcerrors.Internal:
		return true
	case gcerrors.ResourceExhausted:
		return true
	}

	return false
}

func translateError(err error) error {
	switch gcerrors.Code(err) {
	case gcerrors.OK:
		return nil
	case gcerrors.NotFound:
		return blob.ErrBlobNotFound
	}

	return err
}

func (az *azStorage) PutBlob(ctx context.Context, b blob.ID, data []byte) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	throttled, err := az.uploadThrottler.AddReader(ioutil.NopCloser(bytes.NewReader(data)))
	if err != nil {
		return err
	}

	// create azure Bucket writer
	writer, err := az.bucket.NewWriter(ctx, az.getObjectNameString(b), &gblob.WriterOptions{ContentType: "application/x-kopia"})
	if err != nil {
		return err
	}

	_, err = io.Copy(writer, throttled)
	if err != nil {
		// cancel context before closing the writer causes it to abandon the upload.
		cancel()

		_ = writer.Close() // failing already, ignore the error

		return translateError(err)
	}

	// calling close before cancel() causes it to commit the upload.
	return translateError(writer.Close())
}

// DeleteBlob deletes azure blob from container with given ID
func (az *azStorage) DeleteBlob(ctx context.Context, b blob.ID) error {
	attempt := func() (interface{}, error) {
		return nil, az.bucket.Delete(ctx, az.getObjectNameString(b))
	}
	_, err := exponentialBackoff(ctx, fmt.Sprintf("DeleteBlob(%q)", b), attempt)
	err = translateError(err)

	// don't return error if blob is already deleted
	if err == blob.ErrBlobNotFound {
		return nil
	}

	return err
}

func (az *azStorage) getObjectNameString(b blob.ID) string {
	return az.Prefix + string(b)
}

// ListBlobs list azure blobs with given prefix
func (az *azStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	// create list iterator
	li := az.bucket.List(&gblob.ListOptions{Prefix: az.getObjectNameString(prefix)})

	// iterate over list iterator
	for {
		lo, err := li.Next(ctx)
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		bm := blob.Metadata{
			BlobID:    blob.ID(lo.Key[len(az.Prefix):]),
			Length:    lo.Size,
			Timestamp: lo.ModTime,
		}

		if err := callback(bm); err != nil {
			return err
		}
	}

	return nil
}

func (az *azStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   azStorageType,
		Config: &az.Options,
	}
}

func (az *azStorage) Close(ctx context.Context) error {
	return az.bucket.Close()
}

func toBandwidth(bytesPerSecond int) iothrottler.Bandwidth {
	if bytesPerSecond <= 0 {
		return iothrottler.Unlimited
	}

	return iothrottler.Bandwidth(bytesPerSecond) * iothrottler.BytesPerSecond
}

// New creates new Azure Blob Storage-backed storage with specified options:
//
// - the 'Container', 'StorageAccount' and 'StorageKey' fields are required and all other parameters are optional.
func New(ctx context.Context, opt *Options) (blob.Storage, error) {
	if opt.Container == "" {
		return nil, errors.New("container name must be specified")
	}

	// create a credentials object.
	credential, err := azureblob.NewCredential(azureblob.AccountName(opt.StorageAccount), azureblob.AccountKey(opt.StorageKey))
	if err != nil {
		return nil, err
	}

	// create a Pipeline with credentials.
	pipeline := azureblob.NewPipeline(credential, azblob.PipelineOptions{})

	// create a *blob.Bucket.
	bucket, err := azureblob.OpenBucket(ctx, pipeline, azureblob.AccountName(opt.StorageAccount), opt.Container, &azureblob.Options{Credential: credential})
	if err != nil {
		return nil, err
	}

	downloadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxDownloadSpeedBytesPerSecond))
	uploadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxUploadSpeedBytesPerSecond))

	az := &azStorage{
		Options:           *opt,
		ctx:               ctx,
		bucket:            bucket,
		downloadThrottler: downloadThrottler,
		uploadThrottler:   uploadThrottler,
	}

	// verify Azure connection is functional by listing blobs in a bucket, which will fail if the container
	// does not exist. We list with a prefix that will not exist, to avoid iterating through any objects.
	nonExistentPrefix := fmt.Sprintf("kopia-azure-storage-initializing-%v", time.Now().UnixNano()) // allow:no-inject-time
	err = az.ListBlobs(ctx, blob.ID(nonExistentPrefix), func(md blob.Metadata) error {
		return nil
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to list from the bucket")
	}

	return az, nil
}

func init() {
	blob.AddSupportedStorage(
		azStorageType,
		func() interface{} {
			return &Options{}
		},
		func(ctx context.Context, o interface{}) (blob.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
