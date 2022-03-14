// Package sparsefile provides wrappers for handling the writing of sparse files (files with holes).
package sparsefile

import (
	"io"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/stat"
)

// Write writes the contents of src to the given targetPath, omitting any holes.
func Write(targetPath string, src io.Reader, size int64) error {
	dst, err := os.OpenFile(targetPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600) //nolint:gosec,gomnd
	if err != nil {
		return err //nolint:wrapcheck
	}

	// ensure we always close f. Note that this does not conflict with the
	// close below, as close is idempotent.
	defer dst.Close() //nolint:errcheck,gosec

	if err = dst.Truncate(size); err != nil {
		return errors.Wrap(err, "error writing sparse file")
	}

	s, err := stat.GetBlockSize(targetPath)
	if err != nil {
		return errors.Wrap(err, "error writing sparse file")
	}

	buf := iocopy.GetBuffer()
	defer iocopy.ReleaseBuffer(buf)

	w, err := copySparse(dst, src, buf[0:s])
	if err != nil {
		return errors.Wrap(err, "error writing sparse file")
	}

	if w != size {
		return errors.Errorf("")
	}

	if err := dst.Close(); err != nil {
		return err //nolint:wrapcheck
	}

	return nil
}

func copySparse(dst io.WriteSeeker, src io.Reader, buf []byte) (written int64, err error) {
	for {
		nr, er := src.Read(buf)
		if nr > 0 { // nolint:nestif
			// If non-zero data is read, write it. Otherwise, skip forwards.
			if isAllZero(buf) {
				dst.Seek(int64(nr), os.SEEK_CUR) // nolint:errcheck
				written += int64(nr)

				continue
			}

			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0

				if ew == nil {
					ew = errors.New("invalid write result")
				}
			}

			written += int64(nw)

			if ew != nil {
				err = ew
				break
			}

			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}

		if er != nil {
			if er != io.EOF {
				err = er
			}

			break
		}
	}

	return written, err
}

func isAllZero(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}

	return true
}
