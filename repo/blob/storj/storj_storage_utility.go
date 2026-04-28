package storj

import (
	"context"
	"fmt"
	"os"
	"strings"

	"storj.io/storj/cmd/uplink/ulfs"
	"storj.io/uplink"

	"github.com/kopia/kopia/repo/blob"
)

//nolint:unused // might come in handy for bucket management (testing)
func getStorjRemotePath(filePath string) string {
	return fmt.Sprintf("%s%s", storjSchemePfx, filePath)
}

//nolint:unused // might come in handy for bucket management (testing)
func getBucketName(path string) string {
	path = strings.TrimPrefix(path, storjSchemePfx)

	split := strings.Split(path, string(os.PathSeparator))
	if split != nil {
		return split[0]
	}

	return ""
}

//nolint:unused // might come in handy for bucket management (testing)
func getOrCreateBucket(ctx context.Context, project *uplink.Project, bucketName string) (created *uplink.Bucket, err error) {
	bckt, err := project.StatBucket(ctx, bucketName)
	if err == nil && bckt != nil {
		return bckt, nil
	}

	bckt, err = project.CreateBucket(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("creating bucket %q: %w", bucketName, err)
	}

	return bckt, nil
}

//nolint:unused // might come in handy for bucket management (testing)
func listCallBack(item *uplink.Bucket, iter ulfs.ObjectIterator, callback func(blob.Metadata) error) error {
	for iter.Next() {
		obj := iter.Item()

		bm := blob.Metadata{
			BlobID:    blob.ID(obj.Loc.Loc()),
			Length:    obj.ContentLength,
			Timestamp: item.Created,
		}

		if err := callback(bm); err != nil {
			return err
		}
	}

	return nil
}
