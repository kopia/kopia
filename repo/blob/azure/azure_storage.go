// Package azure implements Azure Blob Storage.
package azure

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/pkg/errors"

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

	service azblob.ServiceClient
	bucket  azblob.ContainerClient
}

func (az *azStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	if offset < 0 {
		return errors.Wrap(blob.ErrInvalidRange, "invalid offset")
	}

	bc := az.bucket.NewBlockBlobClient(az.getObjectNameString(b))
	opt := &azblob.DownloadBlobOptions{}

	if length > 0 {
		opt.Offset = &offset
		opt.Count = &length
	}

	if length == 0 {
		l1 := int64(1)
		opt.Offset = &offset
		opt.Count = &l1
	}

	resp, err := bc.Download(ctx, opt)
	if err != nil {
		return translateError(err)
	}

	body := resp.Body(azblob.RetryReaderOptions{})
	defer body.Close() // nolint:errcheck

	if length == 0 {
		return nil
	}

	if err := iocopy.JustCopy(output, body); err != nil {
		return translateError(err)
	}

	// nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func (az *azStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	bc := az.bucket.NewBlockBlobClient(az.getObjectNameString(b))

	fi, err := bc.GetProperties(ctx, nil)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(translateError(err), "Attributes")
	}

	return blob.Metadata{
		BlobID:    b,
		Length:    *fi.ContentLength,
		Timestamp: *fi.LastModified,
	}, nil
}

func translateError(err error) error {
	if err == nil {
		return nil
	}

	var re *azblob.StorageError

	if errors.As(err, &re) {
		// nolint:exhaustive
		switch re.ErrorCode {
		case azblob.StorageErrorCodeBlobNotFound:
			return blob.ErrBlobNotFound
		case azblob.StorageErrorCodeInvalidRange:
			return blob.ErrInvalidRange
		}
	}

	return err
}

func (az *azStorage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	if opts.HasRetentionOptions() {
		return errors.New("setting blob-retention is not supported")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bc := az.bucket.NewBlockBlobClient(az.getObjectNameString(b))

	_, err := bc.Upload(ctx, data.Reader(), &azblob.UploadBlockBlobOptions{})

	return translateError(err)
}

func (az *azStorage) SetTime(ctx context.Context, b blob.ID, t time.Time) error {
	return blob.ErrSetTimeUnsupported
}

// DeleteBlob deletes azure blob from container with given ID.
func (az *azStorage) DeleteBlob(ctx context.Context, b blob.ID) error {
	_, err := az.bucket.NewBlockBlobClient(az.getObjectNameString(b)).Delete(ctx, nil)
	err = translateError(err)

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
	prefixStr := az.Prefix + string(prefix)

	pager := az.bucket.ListBlobsFlat(&azblob.ContainerListBlobFlatSegmentOptions{
		Prefix: &prefixStr,
	})

	for pager.NextPage(ctx) {
		resp := pager.PageResponse()

		for _, it := range resp.ContainerListBlobFlatSegmentResult.Segment.BlobItems {
			bm := blob.Metadata{
				BlobID:    blob.ID((*it.Name)[len(az.Prefix):]),
				Length:    *it.Properties.ContentLength,
				Timestamp: *it.Properties.LastModified,
			}

			if err := callback(bm); err != nil {
				return err
			}
		}
	}

	return translateError(pager.Err())
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
	return nil
}

func (az *azStorage) FlushCaches(ctx context.Context) error {
	return nil
}

// New creates new Azure Blob Storage-backed storage with specified options:
//
// - the 'Container', 'StorageAccount' and 'StorageKey' fields are required and all other parameters are optional.
func New(ctx context.Context, opt *Options) (blob.Storage, error) {
	if opt.Container == "" {
		return nil, errors.New("container name must be specified")
	}

	var (
		service    azblob.ServiceClient
		serviceErr error
	)

	storageDomain := opt.StorageDomain
	if storageDomain == "" {
		storageDomain = "blob.core.windows.net"
	}

	storageHostname := fmt.Sprintf("%v.%v", opt.StorageAccount, storageDomain)

	switch {
	case opt.SASToken != "":
		service, serviceErr = azblob.NewServiceClientWithNoCredential(
			fmt.Sprintf("https://%s?%s", storageHostname, opt.SASToken), nil)

	case opt.StorageKey != "":
		// create a credentials object.
		cred, err := azblob.NewSharedKeyCredential(opt.StorageAccount, opt.StorageKey)
		if err != nil {
			return nil, errors.Wrap(err, "unable to initialize credentials")
		}

		service, serviceErr = azblob.NewServiceClientWithSharedKey(
			fmt.Sprintf("https://%s/", storageHostname), cred, nil,
		)

	default:
		return nil, errors.Errorf("either storage key or SAS token must be provided")
	}

	if serviceErr != nil {
		return nil, errors.Wrap(serviceErr, "opening azure service")
	}

	bucket := service.NewContainerClient(opt.Container)

	raw := &azStorage{
		Options: *opt,
		ctx:     ctx,
		bucket:  bucket,
		service: service,
	}

	az := retrying.NewWrapper(raw)

	// verify Azure connection is functional by listing blobs in a bucket, which will fail if the container
	// does not exist. We list with a prefix that will not exist, to avoid iterating through any objects.
	nonExistentPrefix := fmt.Sprintf("kopia-azure-storage-initializing-%v", clock.Now().UnixNano())
	if err := raw.ListBlobs(ctx, blob.ID(nonExistentPrefix), func(md blob.Metadata) error {
		return nil
	}); err != nil {
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
		func(ctx context.Context, o interface{}, isCreate bool) (blob.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
