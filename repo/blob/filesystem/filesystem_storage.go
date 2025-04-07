// Package filesystem implements filesystem-based Storage.
package filesystem

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/dirutil"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sharded"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("repo/filesystem")

const (
	fsStorageType           = "filesystem"
	tempFileRandomSuffixLen = 8

	fsDefaultFileMode os.FileMode = 0o600
	fsDefaultDirMode  os.FileMode = 0o700
)

type fsStorage struct {
	sharded.Storage
	blob.DefaultProviderImplementation
}

type fsImpl struct {
	Options

	osi osInterface
}

var errRetriableInvalidLength = errors.New("invalid length (retriable)")

func (fs *fsImpl) isRetriable(err error) bool {
	if err == nil {
		return false
	}

	err = errors.Cause(err)

	if fs.osi.IsStale(err) {
		// errors indicative of stale resource handle or invalid
		// descriptors should not be retried
		return false
	}

	if fs.osi.IsNotExist(err) {
		return false
	}

	if fs.osi.IsExist(err) {
		return false
	}

	if fs.osi.IsPathError(err) {
		return true
	}

	if fs.osi.IsLinkError(err) {
		return true
	}

	return errors.Is(err, errRetriableInvalidLength)
}

func (fs *fsImpl) GetBlobFromPath(ctx context.Context, dirPath, path string, offset, length int64, output blob.OutputBuffer) error {
	_ = dirPath

	err := retry.WithExponentialBackoffNoValue(ctx, "GetBlobFromPath:"+path, func() error {
		output.Reset()

		f, err := fs.osi.Open(path)
		if err != nil {
			//nolint:wrapcheck
			return err
		}

		defer f.Close() //nolint:errcheck

		if length < 0 {
			return iocopy.JustCopy(output, f)
		}

		if _, err = f.Seek(offset, io.SeekStart); err != nil {
			// do not wrap seek error, we don't want to retry on it.
			return errors.Errorf("seek error: %v", err)
		}

		if err := iocopy.JustCopy(output, io.LimitReader(f, length)); err != nil {
			//nolint:wrapcheck
			return err
		}

		if int64(output.Length()) != length && length > 0 {
			if runtime.GOOS == "darwin" {
				if st, err := f.Stat(); err == nil && st.Size() == 0 {
					// this sometimes fails on macOS for unknown reasons, likely a bug in the filesystem
					// retry deals with this transient state.
					// see https://github.com/kopia/kopia/issues/299
					return errRetriableInvalidLength
				}
			}

			return errors.New("invalid length")
		}

		return nil
	}, fs.isRetriable)
	if err != nil {
		if fs.osi.IsNotExist(err) {
			return blob.ErrBlobNotFound
		}

		return err
	}

	//nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func (fs *fsImpl) GetMetadataFromPath(ctx context.Context, dirPath, path string) (blob.Metadata, error) {
	_ = dirPath

	//nolint:wrapcheck
	return retry.WithExponentialBackoff(ctx, "GetMetadataFromPath:"+path, func() (blob.Metadata, error) {
		fi, err := fs.osi.Stat(path)
		if err != nil {
			if fs.osi.IsNotExist(err) {
				return blob.Metadata{}, blob.ErrBlobNotFound
			}

			//nolint:wrapcheck
			return blob.Metadata{}, err
		}

		return blob.Metadata{
			Length:    fi.Size(),
			Timestamp: fi.ModTime(),
		}, nil
	}, fs.isRetriable)
}

//nolint:wrapcheck,gocyclo
func (fs *fsImpl) PutBlobInPath(ctx context.Context, dirPath, path string, data blob.Bytes, opts blob.PutOptions) error {
	_ = dirPath

	switch {
	case opts.HasRetentionOptions():
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "blob-retention")
	case opts.DoNotRecreate:
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "do-not-recreate")
	}

	return retry.WithExponentialBackoffNoValue(ctx, "PutBlobInPath:"+path, func() error {
		randSuffix := make([]byte, tempFileRandomSuffixLen)
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

		err = fs.osi.Rename(tempFile, path)
		if err != nil {
			if removeErr := fs.osi.Remove(tempFile); removeErr != nil {
				log(ctx).Errorf("can't remove temp file: %v", removeErr)
			}

			//nolint:wrapcheck
			return err
		}

		if fs.FileUID != nil && fs.FileGID != nil && fs.osi.Geteuid() == 0 {
			if chownErr := fs.osi.Chown(path, *fs.FileUID, *fs.FileGID); chownErr != nil {
				log(ctx).Errorf("can't change file permissions: %v", chownErr)
			}
		}

		if t := opts.SetModTime; !t.IsZero() {
			if chtimesErr := fs.osi.Chtimes(path, t, t); chtimesErr != nil {
				return errors.Wrapf(chtimesErr, "can't change file %q times", path)
			}
		}

		if t := opts.GetModTime; t != nil {
			fi, err := fs.osi.Stat(path)
			if err != nil {
				return errors.Wrapf(err, "can't get mod time for file %q", path)
			}

			*t = fi.ModTime()
		}

		return nil
	}, fs.isRetriable)
}

func (fs *fsImpl) createTempFileAndDir(tempFile string) (osWriteFile, error) {
	f, err := fs.osi.CreateNewFile(tempFile, fs.fileMode())
	if fs.osi.IsNotExist(err) {
		if err = dirutil.MkSubdirAll(fs.osi, fs.Path, filepath.Dir(tempFile), fs.dirMode()); err != nil {
			return nil, errors.Wrap(err, "cannot create directory")
		}

		//nolint:wrapcheck
		return fs.osi.CreateNewFile(tempFile, fs.fileMode())
	}

	if err != nil {
		//nolint:wrapcheck
		return nil, err
	}

	return f, nil
}

func (fs *fsImpl) DeleteBlobInPath(ctx context.Context, dirPath, path string) error {
	_ = dirPath

	//nolint:wrapcheck
	return retry.WithExponentialBackoffNoValue(ctx, "DeleteBlobInPath:"+path, func() error {
		err := fs.osi.Remove(path)
		if err == nil || fs.osi.IsNotExist(err) {
			return nil
		}

		//nolint:wrapcheck
		return err
	}, fs.isRetriable)
}

func (fs *fsImpl) ReadDir(ctx context.Context, dirname string) ([]os.FileInfo, error) {
	entries, err := retry.WithExponentialBackoff(ctx, "ReadDir:"+dirname, func() ([]os.DirEntry, error) {
		v, err := fs.osi.ReadDir(dirname)
		//nolint:wrapcheck
		return v, err
	}, fs.isRetriable)
	if err != nil {
		return nil, err
	}

	fileInfos := make([]os.FileInfo, 0, len(entries))

	for _, e := range entries {
		fi, err := e.Info()

		if fs.osi.IsNotExist(err) {
			// we lost the race, the file was deleted since it was listed.
			continue
		}

		if err != nil {
			//nolint:wrapcheck
			return nil, err
		}

		fileInfos = append(fileInfos, fi)
	}

	return fileInfos, nil
}

// TouchBlob updates file modification time to current time if it's sufficiently old.
func (fs *fsStorage) TouchBlob(ctx context.Context, blobID blob.ID, threshold time.Duration) (time.Time, error) {
	var mtime time.Time

	//nolint:wrapcheck,forcetypeassert
	err := retry.WithExponentialBackoffNoValue(ctx, "TouchBlob", func() error {
		_, path, err := fs.Storage.GetShardedPathAndFilePath(ctx, blobID)
		if err != nil {
			return errors.Wrap(err, "error getting sharded path")
		}

		osi := fs.Impl.(*fsImpl).osi //nolint:forcetypeassert

		st, err := osi.Stat(path)
		if err != nil {
			//nolint:wrapcheck
			return err
		}

		n := clock.Now()
		mtime = st.ModTime()

		age := n.Sub(mtime)
		if age < threshold {
			return nil
		}

		mtime = n

		//nolint:wrapcheck
		return osi.Chtimes(path, n, n)
	}, fs.Impl.(*fsImpl).isRetriable)

	return mtime, err
}

func (fs *fsStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   fsStorageType,
		Config: &fs.Impl.(*fsImpl).Options, //nolint:forcetypeassert
	}
}

func (fs *fsStorage) DisplayName() string {
	return fmt.Sprintf("Filesystem: %v", fs.RootPath)
}

// New creates new filesystem-backed storage in a specified directory.
func New(ctx context.Context, opts *Options, isCreate bool) (blob.Storage, error) {
	var err error

	osi := opts.osInterfaceOverride
	if osi == nil {
		osi = realOS{}
	}

	if isCreate {
		log(ctx).Debugf("creating directory: %v dir mode: %v", opts.Path, opts.dirMode())

		if mkdirErr := osi.MkdirAll(opts.Path, opts.dirMode()); mkdirErr != nil {
			log(ctx).Errorf("unable to create directory: %v", mkdirErr)
		}
	}

	if _, err = osi.Stat(opts.Path); err != nil {
		return nil, errors.Wrap(err, "cannot access storage path")
	}

	return &fsStorage{
		Storage: sharded.New(&fsImpl{*opts, osi}, opts.Path, opts.Options, isCreate),
	}, nil
}

func init() {
	blob.AddSupportedStorage(fsStorageType, Options{}, New)
}
