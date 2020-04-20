package content

import (
	"context"
	"os"
	"path/filepath"

	"github.com/kopia/kopia/internal/ctxutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
)

type cacheKey string

type contentCache interface {
	close()
	getContent(ctx context.Context, cacheKey cacheKey, blobID blob.ID, offset, length int64) ([]byte, error)
}

func newCacheStorageOrNil(ctx context.Context, cacheDir string, maxBytes int64, subdir string) (blob.Storage, error) {
	var cacheStorage blob.Storage

	var err error

	if maxBytes > 0 && cacheDir != "" {
		contentCacheDir := filepath.Join(cacheDir, subdir)

		if _, err = os.Stat(contentCacheDir); os.IsNotExist(err) {
			if mkdirerr := os.MkdirAll(contentCacheDir, 0700); mkdirerr != nil {
				return nil, mkdirerr
			}
		}

		cacheStorage, err = filesystem.New(ctxutil.Detach(ctx), &filesystem.Options{
			Path:            contentCacheDir,
			DirectoryShards: []int{2},
		})
		if err != nil {
			return nil, err
		}
	}

	return cacheStorage, nil
}
