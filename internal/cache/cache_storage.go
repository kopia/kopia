package cache

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/ctxutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
)

// Storage is the storage interface required by the cache and is implemented by the filesystem Storage.
type Storage interface {
	blob.Storage
	TouchBlob(ctx context.Context, contentID blob.ID, threshold time.Duration) error
}

// NewStorageOrNil returns cache.Storage backed by the provided directory.
func NewStorageOrNil(ctx context.Context, cacheDir string, maxBytes int64, subdir string) (Storage, error) {
	var cacheStorage Storage

	var err error

	if maxBytes > 0 && cacheDir != "" {
		contentCacheDir := filepath.Join(cacheDir, subdir)

		if _, err = os.Stat(contentCacheDir); os.IsNotExist(err) {
			if mkdirerr := os.MkdirAll(contentCacheDir, 0o700); mkdirerr != nil {
				return nil, errors.Wrap(mkdirerr, "error creating cache directory")
			}
		}

		fs, err := filesystem.New(ctxutil.Detach(ctx), &filesystem.Options{
			Path:            contentCacheDir,
			DirectoryShards: []int{2},
		})
		if err != nil {
			return nil, errors.Wrap(err, "error initializing filesystem cache")
		}

		cacheStorage = fs.(Storage)
	}

	return cacheStorage, nil
}
