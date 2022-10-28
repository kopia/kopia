// Package azure implements Azure Blob Storage.
package azure

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/timestampmeta"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	azStorageType = "azureBlob"

	timeMapKey = "Kopiamtime" // this must be capital letter followed by lowercase, to comply with AZ tags naming convention.
)

type azStorage struct {
	Options

	service azblob.ServiceClient
	bucket  azblob.ContainerClient
}

func (az *azStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	return blob.Capacity{}, blob.ErrNotAVolume
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

	body := resp.Body(nil)
	defer body.Close() //nolint:errcheck

	if length == 0 {
		return nil
	}

	if err := iocopy.JustCopy(output, body); err != nil {
		return translateError(err)
	}

	//nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func (az *azStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	bc := az.bucket.NewBlockBlobClient(az.getObjectNameString(b))

	fi, err := bc.GetProperties(ctx, &azblob.GetBlobPropertiesOptions{})
	if err != nil {
		return blob.Metadata{}, errors.Wrap(translateError(err), "Attributes")
	}

	bm := blob.Metadata{
		BlobID:    b,
		Length:    *fi.ContentLength,
		Timestamp: *fi.LastModified,
	}

	if t, ok := timestampmeta.FromValue(fi.Metadata[timeMapKey]); ok {
		bm.Timestamp = t
	}

	return bm, nil
}

func translateError(err error) error {
	if err == nil {
		return nil
	}

	var re *azblob.StorageError

	if errors.As(err, &re) {
		//nolint:exhaustive
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
	switch {
	case opts.HasRetentionOptions():
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "blob-retention")
	case opts.DoNotRecreate:
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "do-not-recreate")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bc := az.bucket.NewBlockBlobClient(az.getObjectNameString(b))

	ubo := &azblob.UploadBlockBlobOptions{
		Metadata: timestampmeta.ToMap(opts.SetModTime, timeMapKey),
	}

	resp, err := bc.Upload(ctx, data.Reader(), ubo)
	if err != nil {
		return translateError(err)
	}

	if opts.GetModTime != nil {
		*opts.GetModTime = *resp.LastModified
	}

	return nil
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
		Include: []azblob.ListBlobsIncludeItem{
			"[" + azblob.ListBlobsIncludeItemMetadata + "]",
		},
	})

	for pager.NextPage(ctx) {
		resp := pager.PageResponse()

		for _, it := range resp.Segment.BlobItems {
			n := *it.Name

			bm := blob.Metadata{
				BlobID: blob.ID(n[len(az.Prefix):]),
				Length: *it.Properties.ContentLength,
			}

			// see if we have 'Kopiamtime' metadata, if so - trust it.
			if t, ok := timestampmeta.FromValue(stringDefault(it.Metadata["kopiamtime"], "")); ok {
				bm.Timestamp = t
			} else {
				bm.Timestamp = *it.Properties.LastModified
			}

			if err := callback(bm); err != nil {
				return err
			}
		}
	}

	return translateError(pager.Err())
}

func stringDefault(s *string, def string) string {
	if s == nil {
		return def
	}

	return *s
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
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
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
	blob.AddSupportedStorage(azStorageType, Options{}, New)
}
