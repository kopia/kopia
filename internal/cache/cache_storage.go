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

// Storage is the storage interface required by the cache and is a subset of methods implemented by the filesystem Storage.
type Storage interface {
	GetBlob(ctx context.Context, id blob.ID, offset, length int64, out blob.OutputBuffer) error
	GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error)
	PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error
	DeleteBlob(ctx context.Context, id blob.ID) error
	ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error
	TouchBlob(ctx context.Context, contentID blob.ID, threshold time.Duration) (time.Time, error)
}

// filesystemImplWrapper is a wrapper around the filesystem.Storage that exposes the cache.Storage interface
// but prevents casting to blob.Storage.
type filesystemImplWrapper struct {
	Storage
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

	return filesystemImplWrapper{fs.(Storage)}, errors.Wrap(err, "error initializing filesystem cache") //nolint:forcetypeassert
}
