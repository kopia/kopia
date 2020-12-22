// Package sharded implements common support for sharded blob providers, such as filesystem or webdav.
package sharded

import (
	"context"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

const minShardedBlobIDLength = 20

// Impl must be implemented by underlying provided.
type Impl interface {
	GetBlobFromPath(ctx context.Context, dirPath, filePath string, offset, length int64) ([]byte, error)
	GetMetadataFromPath(ctx context.Context, dirPath, filePath string) (blob.Metadata, error)
	PutBlobInPath(ctx context.Context, dirPath, filePath string, dataSlices blob.Bytes) error
	SetTimeInPath(ctx context.Context, dirPath, filePath string, t time.Time) error
	DeleteBlobInPath(ctx context.Context, dirPath, filePath string) error
	ReadDir(ctx context.Context, path string) ([]os.FileInfo, error)
}

// Storage provides common implementation of sharded storage.
type Storage struct {
	Impl Impl

	RootPath string
	Suffix   string
	Shards   []int
}

// GetBlob implements blob.Storage.
func (s Storage) GetBlob(ctx context.Context, blobID blob.ID, offset, length int64) ([]byte, error) {
	dirPath, filePath := s.GetShardedPathAndFilePath(blobID)
	return s.Impl.GetBlobFromPath(ctx, dirPath, filePath, offset, length)
}

func (s Storage) getBlobIDFromFileName(name string) (blob.ID, bool) {
	if strings.HasSuffix(name, s.Suffix) {
		return blob.ID(name[0 : len(name)-len(s.Suffix)]), true
	}

	return blob.ID(""), false
}

func (s Storage) makeFileName(blobID blob.ID) string {
	return string(blobID) + s.Suffix
}

// ListBlobs implements blob.Storage.
func (s Storage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	var walkDir func(string, string) error

	walkDir = func(directory string, currentPrefix string) error {
		entries, err := s.Impl.ReadDir(ctx, directory)
		if err != nil {
			return errors.Wrap(err, "error reading directory")
		}

		for _, e := range entries {
			// nolint:nestif
			if e.IsDir() {
				var match bool

				newPrefix := currentPrefix + e.Name()
				if len(prefix) > len(newPrefix) {
					match = strings.HasPrefix(string(prefix), newPrefix)
				} else {
					match = strings.HasPrefix(newPrefix, string(prefix))
				}

				if match {
					if err := walkDir(directory+"/"+e.Name(), currentPrefix+e.Name()); err != nil {
						return err
					}
				}
			} else if fullID, ok := s.getBlobIDFromFileName(currentPrefix + e.Name()); ok {
				if strings.HasPrefix(string(fullID), string(prefix)) {
					if err := callback(blob.Metadata{
						BlobID:    fullID,
						Length:    e.Size(),
						Timestamp: e.ModTime(),
					}); err != nil {
						return err
					}
				}
			}
		}

		return nil
	}

	return walkDir(s.RootPath, "")
}

// GetMetadata implements blob.Storage.
func (s Storage) GetMetadata(ctx context.Context, blobID blob.ID) (blob.Metadata, error) {
	dirPath, filePath := s.GetShardedPathAndFilePath(blobID)

	m, err := s.Impl.GetMetadataFromPath(ctx, dirPath, filePath)
	m.BlobID = blobID

	return m, errors.Wrap(err, "error getting metadata")
}

// PutBlob implements blob.Storage.
func (s Storage) PutBlob(ctx context.Context, blobID blob.ID, data blob.Bytes) error {
	dirPath, filePath := s.GetShardedPathAndFilePath(blobID)

	return s.Impl.PutBlobInPath(ctx, dirPath, filePath, data)
}

// SetTime implements blob.Storage.
func (s Storage) SetTime(ctx context.Context, blobID blob.ID, n time.Time) error {
	dirPath, filePath := s.GetShardedPathAndFilePath(blobID)

	return s.Impl.SetTimeInPath(ctx, dirPath, filePath, n)
}

// DeleteBlob implements blob.Storage.
func (s Storage) DeleteBlob(ctx context.Context, blobID blob.ID) error {
	dirPath, filePath := s.GetShardedPathAndFilePath(blobID)
	return s.Impl.DeleteBlobInPath(ctx, dirPath, filePath)
}

func (s Storage) getShardDirectory(blobID blob.ID) (string, blob.ID) {
	shardPath := s.RootPath

	if len(blobID) < minShardedBlobIDLength {
		return shardPath, blobID
	}

	for _, size := range s.Shards {
		shardPath = path.Join(shardPath, string(blobID[0:size]))
		blobID = blobID[size:]
	}

	return shardPath, blobID
}

// GetShardedPathAndFilePath returns the path of the shard and file name within the shard for a given blob ID.
func (s Storage) GetShardedPathAndFilePath(blobID blob.ID) (shardPath, filePath string) {
	shardPath, blobID = s.getShardDirectory(blobID)
	filePath = path.Join(shardPath, s.makeFileName(blobID))

	return
}
