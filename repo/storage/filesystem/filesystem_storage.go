// Package filesystem implements filesystem-based Storage.
package filesystem

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kopia/kopia/internal/kopialogging"
	"github.com/kopia/kopia/repo/storage"
)

var log = kopialogging.Logger("kopia/filesystem")

const (
	fsStorageType        = "filesystem"
	fsStorageChunkSuffix = ".f"
)

var (
	fsDefaultShards               = []int{3, 3}
	fsDefaultFileMode os.FileMode = 0600
	fsDefaultDirMode  os.FileMode = 0700
)

type fsStorage struct {
	Options
}

func (fs *fsStorage) GetBlock(ctx context.Context, blockID string, offset, length int64) ([]byte, error) {
	_, path := fs.getShardedPathAndFilePath(blockID)

	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, storage.ErrBlockNotFound
	}

	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck

	if length < 0 {
		return ioutil.ReadAll(f)
	}

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	return ioutil.ReadAll(io.LimitReader(f, length))
}

func getstringFromFileName(name string) (string, bool) {
	if strings.HasSuffix(name, fsStorageChunkSuffix) {
		return name[0 : len(name)-len(fsStorageChunkSuffix)], true
	}

	return string(""), false
}

func makeFileName(blockID string) string {
	return blockID + fsStorageChunkSuffix
}

func (fs *fsStorage) ListBlocks(ctx context.Context, prefix string, callback func(storage.BlockMetadata) error) error {
	var walkDir func(string, string) error

	walkDir = func(directory string, currentPrefix string) error {
		entries, err := ioutil.ReadDir(directory)
		if err != nil {
			return err
		}

		for _, e := range entries {
			if e.IsDir() {
				newPrefix := currentPrefix + e.Name()
				var match bool

				if len(prefix) > len(newPrefix) {
					match = strings.HasPrefix(prefix, newPrefix)
				} else {
					match = strings.HasPrefix(newPrefix, prefix)
				}

				if match {
					if err := walkDir(directory+"/"+e.Name(), currentPrefix+e.Name()); err != nil {
						return err
					}
				}
			} else if fullID, ok := getstringFromFileName(currentPrefix + e.Name()); ok {
				if strings.HasPrefix(fullID, prefix) {
					if err := callback(storage.BlockMetadata{
						BlockID:   fullID,
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

	return walkDir(fs.Path, "")
}

// TouchBlock updates file modification time to current time if it's sufficiently old.
func (fs *fsStorage) TouchBlock(ctx context.Context, blockID string, threshold time.Duration) error {
	_, path := fs.getShardedPathAndFilePath(blockID)
	st, err := os.Stat(path)
	if err != nil {
		return err
	}

	n := time.Now()
	age := n.Sub(st.ModTime())
	if age < threshold {
		return nil
	}

	log.Debugf("updating timestamp on %v to %v", path, n)
	return os.Chtimes(path, n, n)
}

func (fs *fsStorage) PutBlock(ctx context.Context, blockID string, data []byte) error {
	_, path := fs.getShardedPathAndFilePath(blockID)

	tempFile := fmt.Sprintf("%s.tmp.%d", path, rand.Int())
	f, err := fs.createTempFileAndDir(tempFile)
	if err != nil {
		return fmt.Errorf("cannot create temporary file: %v", err)
	}

	if _, err = f.Write(data); err != nil {
		return fmt.Errorf("can't write temporary file: %v", err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("can't close temporary file: %v", err)
	}

	err = os.Rename(tempFile, path)
	if err != nil {
		if removeErr := os.Remove(tempFile); removeErr != nil {
			log.Warningf("can't remove temp file: %v", removeErr)
		}
		return err
	}

	if fs.FileUID != nil && fs.FileGID != nil && os.Geteuid() == 0 {
		if chownErr := os.Chown(path, *fs.FileUID, *fs.FileGID); chownErr != nil {
			log.Warningf("can't change file permissions: %v", chownErr)
		}
	}

	return nil
}

func (fs *fsStorage) createTempFileAndDir(tempFile string) (*os.File, error) {
	flags := os.O_CREATE | os.O_WRONLY | os.O_EXCL
	f, err := os.OpenFile(tempFile, flags, fs.fileMode())
	if os.IsNotExist(err) {
		if err = os.MkdirAll(filepath.Dir(tempFile), fs.dirMode()); err != nil {
			return nil, fmt.Errorf("cannot create directory: %v", err)
		}
		return os.OpenFile(tempFile, flags, fs.fileMode())
	}

	return f, err
}

func (fs *fsStorage) DeleteBlock(ctx context.Context, blockID string) error {
	_, path := fs.getShardedPathAndFilePath(blockID)
	err := os.Remove(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}

	return err
}

func (fs *fsStorage) getShardDirectory(blockID string) (string, string) {
	shardPath := fs.Path
	if len(blockID) < 20 {
		return shardPath, blockID
	}
	for _, size := range fs.shards() {
		shardPath = filepath.Join(shardPath, blockID[0:size])
		blockID = blockID[size:]
	}

	return shardPath, blockID
}

func (fs *fsStorage) getShardedPathAndFilePath(blockID string) (string, string) {
	shardPath, blockID := fs.getShardDirectory(blockID)
	result := filepath.Join(shardPath, makeFileName(blockID))
	return shardPath, result
}

func (fs *fsStorage) ConnectionInfo() storage.ConnectionInfo {
	return storage.ConnectionInfo{
		Type:   fsStorageType,
		Config: &fs.Options,
	}
}

func (fs *fsStorage) Close(ctx context.Context) error {
	return nil
}

// New creates new filesystem-backed storage in a specified directory.
func New(ctx context.Context, opts *Options) (storage.Storage, error) {
	var err error

	if _, err = os.Stat(opts.Path); err != nil {
		return nil, fmt.Errorf("cannot access storage path: %v", err)
	}

	r := &fsStorage{
		Options: *opts,
	}

	return r, nil
}

func init() {
	storage.AddSupportedStorage(
		fsStorageType,
		func() interface{} { return &Options{} },
		func(ctx context.Context, o interface{}) (storage.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
