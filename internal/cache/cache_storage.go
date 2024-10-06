package cache

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/blob/sharded"
)

//nolint:gochecknoglobals
var mkdirAll = os.MkdirAll // for testability

// DirMode is the directory mode for all caches.
const DirMode = 0o700

// Storage is the storage interface required by the cache and is implemented by the filesystem Storage.
type Storage interface {
	blob.Storage
	TouchBlob(ctx context.Context, contentID blob.ID, threshold time.Duration) (time.Time, error)
}

// NewStorageOrNil returns cache.Storage backed by the provided directory.
func NewStorageOrNil(ctx context.Context, cacheDir string, maxBytes int64, subdir string) (Storage, error) {
	if maxBytes <= 0 || cacheDir == "" {
		return nil, nil
	}

	if !ospath.IsAbs(cacheDir) {
		return nil, errors.Errorf("cache dir %q was not absolute", cacheDir)
	}

	contentCacheDir := filepath.Join(cacheDir, subdir)

	if _, err := os.Stat(contentCacheDir); os.IsNotExist(err) {
		if mkdirerr := mkdirAll(contentCacheDir, DirMode); mkdirerr != nil {
			return nil, errors.Wrap(mkdirerr, "error creating cache directory")
		}
	}

	fs, err := filesystem.New(context.WithoutCancel(ctx), &filesystem.Options{
		Path: contentCacheDir,
		Options: sharded.Options{
			DirectoryShards: []int{2},
		},
	}, false)

	return fs.(Storage), errors.Wrap(err, "error initializing filesystem cache") //nolint:forcetypeassert
}
