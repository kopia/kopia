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
	"runtime"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sharded"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.GetContextLoggerFunc("repo/filesystem")

const (
	fsStorageType        = "filesystem"
	fsStorageChunkSuffix = ".f"

	fsDefaultFileMode os.FileMode = 0o600
	fsDefaultDirMode  os.FileMode = 0o700
)

var fsDefaultShards = []int{3, 3}

type fsStorage struct {
	sharded.Storage
}

type fsImpl struct {
	Options
}

var errRetriableInvalidLength = errors.Errorf("invalid length (retriable)")

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

	// retry errors during file operations
	var pe *os.PathError
	if errors.As(err, &pe) {
		return true
	}

	// retry errors during rename
	var le *os.LinkError
	if errors.As(err, &le) {
		return true
	}

	return errors.Is(err, errRetriableInvalidLength)
}

func (fs *fsImpl) GetBlobFromPath(ctx context.Context, dirPath, path string, offset, length int64) ([]byte, error) {
	val, err := retry.WithExponentialBackoff(ctx, "GetBlobFromPath:"+path, func() (interface{}, error) {
		f, err := os.Open(path) //nolint:gosec
		if err != nil {
			//nolint:wrapcheck
			return nil, err
		}

		defer f.Close() //nolint:errcheck,gosec

		if length < 0 {
			return ioutil.ReadAll(f)
		}

		if _, err = f.Seek(offset, io.SeekStart); err != nil {
			// do not wrap seek error, we don't want to retry on it.
			return nil, errors.Errorf("seek error: %v", err)
		}

		b, err := ioutil.ReadAll(io.LimitReader(f, length))
		if err != nil {
			//nolint:wrapcheck
			return nil, err
		}

		if int64(len(b)) != length && length > 0 {
			if runtime.GOOS == "darwin" {
				if st, err := f.Stat(); err == nil && st.Size() == 0 {
					// this sometimes fails on macOS for unknown reasons, likely a bug in the filesystem
					// retry deals with this transient state.
					// see see https://github.com/kopia/kopia/issues/299
					// nolint:wrapcheck
					return nil, errRetriableInvalidLength
				}
			}

			return nil, errors.Errorf("invalid length")
		}

		return b, nil
	}, isRetriable)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, blob.ErrBlobNotFound
		}

		// nolint:wrapcheck
		return nil, err
	}

	return blob.EnsureLengthExactly(val.([]byte), length)
}

func (fs *fsImpl) GetMetadataFromPath(ctx context.Context, dirPath, path string) (blob.Metadata, error) {
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return blob.Metadata{}, blob.ErrBlobNotFound
		}

		// nolint:wrapcheck
		return blob.Metadata{}, err
	}

	return blob.Metadata{
		Length:    fi.Size(),
		Timestamp: fi.ModTime(),
	}, nil
}

func (fs *fsImpl) PutBlobInPath(ctx context.Context, dirPath, path string, data blob.Bytes) error {
	return retry.WithExponentialBackoffNoValue(ctx, "PutBlobInPath:"+path, func() error {
		randSuffix := make([]byte, 8)
		if _, err := rand.Read(randSuffix); err != nil {
			return errors.Wrap(err, "can't get random bytes")
		}

		tempFile := fmt.Sprintf("%s.tmp.%x", path, randSuffix)

		f, err := fs.createTempFileAndDir(tempFile)
		if err != nil {
			return errors.Wrap(err, "cannot create temporary file")
		}

		if _, err = data.WriteTo(f); err != nil {
			return errors.Wrap(err, "can't write temporary file")
		}

		if err = f.Close(); err != nil {
			return errors.Wrap(err, "can't close temporary file")
		}

		err = os.Rename(tempFile, path)
		if err != nil {
			if removeErr := os.Remove(tempFile); removeErr != nil {
				log(ctx).Errorf("can't remove temp file: %v", removeErr)
			}

			// nolint:wrapcheck
			return err
		}

		if fs.FileUID != nil && fs.FileGID != nil && os.Geteuid() == 0 {
			if chownErr := os.Chown(path, *fs.FileUID, *fs.FileGID); chownErr != nil {
				log(ctx).Errorf("can't change file permissions: %v", chownErr)
			}
		}

		return nil
	}, isRetriable)
}

func (fs *fsImpl) createTempFileAndDir(tempFile string) (*os.File, error) {
	flags := os.O_CREATE | os.O_WRONLY | os.O_EXCL

	f, err := os.OpenFile(tempFile, flags, fs.fileMode()) //nolint:gosec
	if os.IsNotExist(err) {
		if err = os.MkdirAll(filepath.Dir(tempFile), fs.dirMode()); err != nil {
			return nil, errors.Wrap(err, "cannot create directory")
		}

		return os.OpenFile(tempFile, flags, fs.fileMode()) //nolint:gosec
	}

	// nolint:wrapcheck
	return f, err
}

func (fs *fsImpl) DeleteBlobInPath(ctx context.Context, dirPath, path string) error {
	return retry.WithExponentialBackoffNoValue(ctx, "DeleteBlobInPath:"+path, func() error {
		err := os.Remove(path)
		if err == nil || os.IsNotExist(err) {
			return nil
		}

		// nolint:wrapcheck
		return err
	}, isRetriable)
}

func (fs *fsImpl) ReadDir(ctx context.Context, dirname string) ([]os.FileInfo, error) {
	v, err := retry.WithExponentialBackoff(ctx, "ReadDir:"+dirname, func() (interface{}, error) {
		v, err := ioutil.ReadDir(dirname)
		// nolint:wrapcheck
		return v, err
	}, isRetriable)
	if err != nil {
		// nolint:wrapcheck
		return nil, err
	}

	return v.([]os.FileInfo), nil
}

// SetTime updates file modification time to the provided time.
func (fs *fsImpl) SetTimeInPath(ctx context.Context, dirPath, filePath string, n time.Time) error {
	log(ctx).Debugf("updating timestamp on %v to %v", filePath, n)

	return os.Chtimes(filePath, n, n)
}

// TouchBlob updates file modification time to current time if it's sufficiently old.
func (fs *fsStorage) TouchBlob(ctx context.Context, blobID blob.ID, threshold time.Duration) error {
	_, path := fs.Storage.GetShardedPathAndFilePath(blobID)

	st, err := os.Stat(path)
	if err != nil {
		// nolint:wrapcheck
		return err
	}

	n := clock.Now()

	age := n.Sub(st.ModTime())
	if age < threshold {
		return nil
	}

	log(ctx).Debugf("updating timestamp on %v to %v", path, n)

	return os.Chtimes(path, n, n)
}

func (fs *fsStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   fsStorageType,
		Config: &fs.Impl.(*fsImpl).Options,
	}
}

func (fs *fsStorage) DisplayName() string {
	return fmt.Sprintf("Filesystem: %v", fs.RootPath)
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
