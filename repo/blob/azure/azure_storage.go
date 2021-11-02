// Package azure implements Azure Blob Storage.
package azure

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/efarrer/iothrottler"
	"github.com/pkg/errors"
	gblob "gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
	"gocloud.dev/gcerrors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
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

func (az *azStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	if offset < 0 {
		return errors.Wrap(blob.ErrInvalidRange, "invalid offset")
	}

	attempt := func() error {
		reader, err := az.bucket.NewRangeReader(ctx, az.getObjectNameString(b), offset, length, nil)
		if err != nil {
			return errors.Wrap(err, "NewRangeReader")
		}

		defer reader.Close() //nolint:errcheck

		throttled, err := az.downloadThrottler.AddReader(reader)
		if err != nil {
			return errors.Wrap(err, "AddReader")
		}

		// nolint:wrapcheck
		return iocopy.JustCopy(output, throttled)
	}

	if err := attempt(); err != nil {
		return translateError(err)
	}

	// nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func (az *azStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	fi, err := az.bucket.Attributes(ctx, az.getObjectNameString(b))
	if err != nil {
		return blob.Metadata{}, errors.Wrap(translateError(err), "Attributes")
	}

	return blob.Metadata{
		BlobID:    b,
		Length:    fi.Size,
		Timestamp: fi.ModTime,
	}, nil
}

func translateError(err error) error {
	if err == nil {
		return nil
	}

	var re azblob.ResponseError
	if errors.As(err, &re) {
		if re.Response().StatusCode == http.StatusRequestedRangeNotSatisfiable { //nolint:bodyclose
			return blob.ErrInvalidRange
		}
	}

	switch gcerrors.Code(err) {
	case gcerrors.OK:
		return nil
	case gcerrors.NotFound:
		return blob.ErrBlobNotFound
	case gcerrors.InvalidArgument:
		return blob.ErrInvalidRange
	default:
		return err
	}
}

func (az *azStorage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	throttled, err := az.uploadThrottler.AddReader(io.NopCloser(data.Reader()))
	if err != nil {
		// nolint:wrapcheck
		return err
	}

	// create azure Bucket writer
	writer, err := az.bucket.NewWriter(ctx, az.getObjectNameString(b), &gblob.WriterOptions{ContentType: "application/x-kopia"})
	if err != nil {
		// nolint:wrapcheck
		return err
	}

	if err := iocopy.JustCopy(writer, throttled); err != nil {
		// cancel context before closing the writer causes it to abandon the upload.
		cancel()

		_ = writer.Close() // failing already, ignore the error

		return translateError(err)
	}

	// calling close before cancel() causes it to commit the upload.
	return translateError(writer.Close())
}

func (az *azStorage) SetTime(ctx context.Context, b blob.ID, t time.Time) error {
	return blob.ErrSetTimeUnsupported
}

// DeleteBlob deletes azure blob from container with given ID.
func (az *azStorage) DeleteBlob(ctx context.Context, b blob.ID) error {
	err := translateError(az.bucket.Delete(ctx, az.getObjectNameString(b)))

	// don't return error if blob is already deleted
	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}

	return err
}

func (az *azStorage) getObjectNameString(b blob.ID) string {
	return az.Prefix + string(b)
}

// ListBlobs list azure blobs with given prefix.
func (az *azStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	// create list iterator
	li := az.bucket.List(&gblob.ListOptions{Prefix: az.getObjectNameString(prefix)})

	// iterate over list iterator
	for {
		lo, err := li.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			// nolint:wrapcheck
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

func (az *azStorage) DisplayName() string {
	return fmt.Sprintf("Azure: %v", az.Options.Container)
}

func (az *azStorage) Close(ctx context.Context) error {
	return errors.Wrap(az.bucket.Close(), "error closing bucket")
}

func (az *azStorage) FlushCaches(ctx context.Context) error {
	return nil
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

	var (
		abo          azureblob.Options
		pl           pipeline.Pipeline
		pipelineOpts azblob.PipelineOptions
		account      = azureblob.AccountName(opt.StorageAccount)
	)

	if opt.SASToken != "" {
		abo.SASToken = azureblob.SASToken(opt.SASToken)
		// don't set abo.Credential
		pl = azureblob.NewPipeline(azblob.NewAnonymousCredential(), pipelineOpts)
	} else {
		if opt.StorageKey == "" {
			return nil, errors.Errorf("either storage key or SAS token must be provided")
		}

		// create a credentials object.
		cred, err := azureblob.NewCredential(account, azureblob.AccountKey(opt.StorageKey))
		if err != nil {
			return nil, errors.Wrap(err, "unable to initialize credentials")
		}

		abo.Credential = cred
		pl = azureblob.NewPipeline(cred, pipelineOpts)
	}

	abo.StorageDomain = azureblob.StorageDomain(opt.StorageDomain)

	// create a *blob.Bucket.
	bucket, err := azureblob.OpenBucket(ctx, pl, account, opt.Container, &abo)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open bucket")
	}

	downloadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxDownloadSpeedBytesPerSecond))
	uploadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxUploadSpeedBytesPerSecond))

	az := retrying.NewWrapper(&azStorage{
		Options:           *opt,
		ctx:               ctx,
		bucket:            bucket,
		downloadThrottler: downloadThrottler,
		uploadThrottler:   uploadThrottler,
	})

	// verify Azure connection is functional by listing blobs in a bucket, which will fail if the container
	// does not exist. We list with a prefix that will not exist, to avoid iterating through any objects.
	nonExistentPrefix := fmt.Sprintf("kopia-azure-storage-initializing-%v", clock.Now().UnixNano())
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
