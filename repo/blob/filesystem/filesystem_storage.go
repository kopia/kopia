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

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/repologging"
	"github.com/kopia/kopia/repo/blob"
)

var log = repologging.Logger("repo/filesystem")

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

func (fs *fsStorage) GetBlob(ctx context.Context, blobID blob.ID, offset, length int64) ([]byte, error) {
	_, path := fs.getShardedPathAndFilePath(blobID)

	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, blob.ErrBlobNotFound
	}

	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck

	if length < 0 {
		return ioutil.ReadAll(f)
	}

	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(io.LimitReader(f, length))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != length {
		return nil, errors.Errorf("invalid length")
	}
	return b, nil
}

func getBlobIDFromFileName(name string) (blob.ID, bool) {
	if strings.HasSuffix(name, fsStorageChunkSuffix) {
		return blob.ID(name[0 : len(name)-len(fsStorageChunkSuffix)]), true
	}

	return blob.ID(""), false
}

func makeFileName(blobID blob.ID) string {
	return string(blobID) + fsStorageChunkSuffix
}

func (fs *fsStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
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
					match = strings.HasPrefix(string(prefix), newPrefix)
				} else {
					match = strings.HasPrefix(newPrefix, string(prefix))
				}

				if match {
					if err := walkDir(directory+"/"+e.Name(), currentPrefix+e.Name()); err != nil {
						return err
					}
				}
			} else if fullID, ok := getBlobIDFromFileName(currentPrefix + e.Name()); ok {
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

	return walkDir(fs.Path, "")
}

// TouchBlob updates file modification time to current time if it's sufficiently old.
func (fs *fsStorage) TouchBlob(ctx context.Context, blobID blob.ID, threshold time.Duration) error {
	_, path := fs.getShardedPathAndFilePath(blobID)
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

func (fs *fsStorage) PutBlob(ctx context.Context, blobID blob.ID, data []byte) error {
	_, path := fs.getShardedPathAndFilePath(blobID)

	tempFile := fmt.Sprintf("%s.tmp.%d", path, rand.Int())
	f, err := fs.createTempFileAndDir(tempFile)
	if err != nil {
		return errors.Wrap(err, "cannot create temporary file")
	}

	if _, err = f.Write(data); err != nil {
		return errors.Wrap(err, "can't write temporary file")
	}
	if err = f.Close(); err != nil {
		return errors.Wrap(err, "can't close temporary file")
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
			return nil, errors.Wrap(err, "cannot create directory")
		}
		return os.OpenFile(tempFile, flags, fs.fileMode())
	}

	return f, err
}

func (fs *fsStorage) DeleteBlob(ctx context.Context, blobID blob.ID) error {
	_, path := fs.getShardedPathAndFilePath(blobID)
	err := os.Remove(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}

	return err
}

func (fs *fsStorage) getShardDirectory(blobID blob.ID) (string, blob.ID) {
	shardPath := fs.Path
	if len(blobID) < 20 {
		return shardPath, blobID
	}
	for _, size := range fs.shards() {
		shardPath = filepath.Join(shardPath, string(blobID[0:size]))
		blobID = blobID[size:]
	}

	return shardPath, blobID
}

func (fs *fsStorage) getShardedPathAndFilePath(blobID blob.ID) (string, string) {
	shardPath, blobID := fs.getShardDirectory(blobID)
	result := filepath.Join(shardPath, makeFileName(blobID))
	return shardPath, result
}

func (fs *fsStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   fsStorageType,
		Config: &fs.Options,
	}
}

func (fs *fsStorage) Close(ctx context.Context) error {
	return nil
}

// New creates new filesystem-backed storage in a specified directory.
func New(ctx context.Context, opts *Options) (blob.Storage, error) {
	var err error

	if _, err = os.Stat(opts.Path); err != nil {
		return nil, errors.Wrap(err, "cannot access storage path")
	}

	r := &fsStorage{
		Options: *opts,
	}

	return r, nil
}

func init() {
	blob.AddSupportedStorage(
		fsStorageType,
		func() interface{} { return &Options{} },
		func(ctx context.Context, o interface{}) (blob.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
