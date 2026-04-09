package content

import (
	stderrors "errors"
	"io"
	"io/fs"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/cache"
)

type file interface {
	io.WriteCloser
	Name() string
	Sync() error
}

type fsInterface interface {
	CreateTemp(dir, pattern string) (file, error)
	Remove(name string) error
	MkdirAll(path string, perm fs.FileMode) error
}

type localFS struct{}

func (l localFS) CreateTemp(dir, pattern string) (file, error) {
	return os.CreateTemp(dir, pattern) //nolint:wrapcheck
}

func (l localFS) Remove(name string) error {
	return os.Remove(name) //nolint:wrapcheck
}

func (l localFS) MkdirAll(dirPath string, perm fs.FileMode) error {
	return os.MkdirAll(dirPath, perm) //nolint:wrapcheck
}

func writeTempFileAtomic(fsi fsInterface, dirname string, data []byte) (filename string, err error) {
	// write to a temp file to avoid race where two processes are writing at the same time.
	tf, err2 := fsi.CreateTemp(dirname, "tmp")
	if err2 != nil {
		if os.IsNotExist(err2) {
			if mdErr := fsi.MkdirAll(dirname, cache.DirMode); mdErr != nil {
				return "", stderrors.Join(errors.Wrap(mdErr, "cannot create parent directory for temp file"),
					errors.Wrap(err2, "cannot create temp file"))
			}

			tf, err2 = fsi.CreateTemp(dirname, "tmp")
		}
	}

	if err2 != nil {
		return "", errors.Wrap(err2, "can't create tmp file")
	}

	defer func() {
		if cerr := tf.Close(); cerr != nil {
			err = stderrors.Join(err, errors.Wrap(cerr, "can't close tmp file"))
		}

		if err != nil {
			// remove tmp file on error to avoid leaving them behind
			if rerr := fsi.Remove(tf.Name()); rerr != nil {
				err = stderrors.Join(err, errors.Wrap(rerr, "can't remove tmp file"))
			}

			filename = ""
		}
	}()

	if _, err2 := tf.Write(data); err2 != nil {
		return "", errors.Wrap(err2, "can't write to temp file")
	}

	if err2 := tf.Sync(); err2 != nil {
		return "", errors.Wrapf(err2, "cannot sync temporary file in dir %s", dirname)
	}

	return tf.Name(), nil
}
