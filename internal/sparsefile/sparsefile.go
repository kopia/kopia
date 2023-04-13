// Package sparsefile provides wrappers for handling the writing of sparse files (files with holes).
package sparsefile

import (
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
)

// Copy copies a file sparsely (omitting holes) from src to dst, while recycling
// shared buffers.
func Copy(dst io.WriteSeeker, src io.Reader, bufSize uint64) (int64, error) {
	buf := iocopy.GetBuffer()
	defer iocopy.ReleaseBuffer(buf)

	return copyBuffer(dst, src, buf[0:bufSize])
}

// Copy copies bits from src to dst, seeking past blocks of zero bits in src. These
// blocks are omitted, creating a file with holes in dst.
func copyBuffer(dst io.WriteSeeker, src io.Reader, buf []byte) (written int64, err error) {
	for {
		nr, er := src.Read(buf)
		if nr > 0 { //nolint:nestif
			// If non-zero data is read, write it. Otherwise, skip forwards.
			if isAllZero(buf) {
				dst.Seek(int64(nr), io.SeekCurrent) //nolint:errcheck
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
