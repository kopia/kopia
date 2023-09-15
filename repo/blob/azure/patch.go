package azure

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
	"github.com/pkg/errors"
)

// NewWithClient creates new Azure backend storage with the specified client
func NewWithClient(ctx context.Context, opt *Options, client *azblob.Client) (blob.Storage, error) {
	raw := &azStorage{
		Options:   *opt,
		container: opt.Container,
		service:   client,
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
