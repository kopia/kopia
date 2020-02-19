// Package filesystem implements filesystem-based Storage.
package filesystem

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/repologging"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sharded"
)

var log = repologging.Logger("repo/filesystem")

const (
	fsStorageType        = "filesystem"
	fsStorageChunkSuffix = ".f"

	fsDefaultFileMode os.FileMode = 0600
	fsDefaultDirMode  os.FileMode = 0700
)

var fsDefaultShards = []int{3, 3}

type fsStorage struct {
	sharded.Storage
}

type fsImpl struct {
	Options
}

func isRetriable(err error) bool {
	if err == nil {
		return false
	}

	err = errors.Cause(err)

	if os.IsNotExist(err) {
		return false
	}

	if os.IsExist(err) {
		return false
	}

	return true
}

func (fs *fsImpl) GetBlobFromPath(ctx context.Context, dirPath, path string, offset, length int64) ([]byte, error) {
	val, err := retry.WithExponentialBackoff("GetBlobFromPath:"+path, func() (interface{}, error) {
		f, err := os.Open(path) //nolint:gosec
		return f, err
	}, isRetriable)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, blob.ErrBlobNotFound
		}

		return nil, err
	}

	f := val.(*os.File)

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

func (fs *fsImpl) PutBlobInPath(ctx context.Context, dirPath, path string, data []byte) error {
	return retry.WithExponentialBackoffNoValue("PutBlobInPath:"+path, func() error {
		randSuffix := make([]byte, 8)
		if _, err := rand.Read(randSuffix); err != nil {
			return errors.Wrap(err, "can't get random bytes")
		}

		progressCallback := blob.ProgressCallback(ctx)

		if progressCallback != nil {
			progressCallback(path, 0, int64(len(data)))
			defer progressCallback(path, int64(len(data)), int64(len(data)))
		}

		tempFile := fmt.Sprintf("%s.tmp.%x", path, randSuffix)

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
	}, isRetriable)
}

func (fs *fsImpl) createTempFileAndDir(tempFile string) (*os.File, error) {
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

func (fs *fsImpl) DeleteBlobInPath(ctx context.Context, dirPath, path string) error {
	return retry.WithExponentialBackoffNoValue("DeleteBlobInPath:"+path, func() error {
		err := os.Remove(path)
		if err == nil || os.IsNotExist(err) {
			return nil
		}

		return err
	}, isRetriable)
}

func (fs *fsImpl) ReadDir(ctx context.Context, dirname string) ([]os.FileInfo, error) {
	v, err := retry.WithExponentialBackoff("ReadDir:"+dirname, func() (interface{}, error) {
		v, err := ioutil.ReadDir(dirname)
		return v, err
	}, isRetriable)

	if err != nil {
		return nil, err
	}

	return v.([]os.FileInfo), nil
}

// TouchBlob updates file modification time to current time if it's sufficiently old.
func (fs *fsStorage) TouchBlob(ctx context.Context, blobID blob.ID, threshold time.Duration) error {
	_, path := fs.Storage.GetShardedPathAndFilePath(blobID)

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

func (fs *fsStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   fsStorageType,
		Config: &fs.Impl.(*fsImpl).Options,
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

	return &fsStorage{
		sharded.Storage{
			Impl:     &fsImpl{Options: *opts},
			RootPath: opts.Path,
			Suffix:   fsStorageChunkSuffix,
			Shards:   opts.shards(),
		},
	}, nil
}

func init() {
	blob.AddSupportedStorage(
		fsStorageType,
		func() interface{} { return &Options{} },
		func(ctx context.Context, o interface{}) (blob.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
