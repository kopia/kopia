package sharded

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/kopia/kopia/repo/blob"
)

type Impl interface {
	GetBlobFromPath(ctx context.Context, dirPath, filePath string, offset, length int64) ([]byte, error)
	PutBlobInPath(ctx context.Context, dirPath, filePath string, data []byte) error
	DeleteBlobInPath(ctx context.Context, dirPath, filePath string) error
	ReadDir(ctx context.Context, path string) ([]os.FileInfo, error)
}

type Storage struct {
	Impl Impl

	RootPath string
	Suffix   string
	Shards   []int
}

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

func (s Storage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	var walkDir func(string, string) error

	walkDir = func(directory string, currentPrefix string) error {
		entries, err := s.Impl.ReadDir(ctx, directory)
		if err != nil {
			return err
		}

		for _, e := range entries {
			if e.IsDir() {
				newPrefix := currentPrefix + e.Name()
				var match bool

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

func (s Storage) PutBlob(ctx context.Context, blobID blob.ID, data []byte) error {
	dirPath, filePath := s.GetShardedPathAndFilePath(blobID)

	return s.Impl.PutBlobInPath(ctx, dirPath, filePath, data)
}

func (s Storage) DeleteBlob(ctx context.Context, blobID blob.ID) error {
	dirPath, filePath := s.GetShardedPathAndFilePath(blobID)
	return s.Impl.DeleteBlobInPath(ctx, dirPath, filePath)
}

func (s Storage) getShardDirectory(blobID blob.ID) (string, blob.ID) {
	shardPath := s.RootPath
	if len(blobID) < 20 {
		return shardPath, blobID
	}
	for _, size := range s.Shards {
		shardPath = filepath.Join(shardPath, string(blobID[0:size]))
		blobID = blobID[size:]
	}

	return shardPath, blobID
}

func (s Storage) GetShardedPathAndFilePath(blobID blob.ID) (shardPath, filePath string) {
	shardPath, blobID = s.getShardDirectory(blobID)
	filePath = filepath.Join(shardPath, s.makeFileName(blobID))
	return
}
