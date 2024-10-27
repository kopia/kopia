// Package autodownload combines and replaces curl, tar and gunzip and sha256sum and allows downloading,
// verifying and extracting the archive (zip, tar, tar.gz) to a local directory without using external tools.
package autodownload

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const dirMode = 0o750

func createFile(target string, mode os.FileMode, modTime time.Time, src io.Reader) error {
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode) //nolint:gosec
	if err != nil {
		return errors.Wrap(err, "error creating file")
	}

	defer os.Chtimes(target, modTime, modTime) //nolint:errcheck

	defer f.Close() //nolint:errcheck

	if _, err := io.Copy(f, src); err != nil {
		return errors.Wrap(err, "error copying contents")
	}

	return nil
}

func createSymlink(linkPath, linkTarget string) error {
	os.Remove(linkPath) //nolint:errcheck

	return errors.Wrap(os.Symlink(linkTarget, linkPath), "error creating symlink")
}

func joinAndStripPath(dir, fname string, stripPathComponents int) (string, bool) {
	parts := strings.Split(filepath.ToSlash(fname), "/")
	if len(parts) <= stripPathComponents {
		return "", false
	}

	parts = parts[stripPathComponents:]

	return filepath.Join(append([]string{dir}, parts...)...), true
}

func untar(dir string, r io.Reader, stripPathComponents int) error {
	var (
		err    error
		header *tar.Header
	)

	tr := tar.NewReader(r)

	for header, err = tr.Next(); err == nil; header, err = tr.Next() {
		if header == nil {
			continue
		}

		target, ok := joinAndStripPath(dir, header.Name, stripPathComponents)
		if !ok {
			continue
		}

		if derr := os.MkdirAll(filepath.Dir(target), dirMode); derr != nil {
			return errors.Wrap(derr, "error creating parent directory")
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if derr := os.MkdirAll(target, dirMode); derr != nil {
				return errors.Wrap(derr, "error creating directory")
			}

		case tar.TypeReg:
			//nolint:gosec
			if ferr := createFile(target, os.FileMode(header.Mode), header.ModTime, tr); ferr != nil {
				return errors.Wrapf(ferr, "error creating file %v", target)
			}

		case tar.TypeSymlink:
			if ferr := createSymlink(target, header.Linkname); ferr != nil {
				return errors.Wrapf(ferr, "error creating file %v", target)
			}

		default:
			return errors.Errorf("unsupported tar entry: %v %v", header.Name, header.Typeflag)
		}
	}

	if errors.Is(err, io.EOF) {
		return nil
	}

	return errors.Wrap(err, "error processing tar archive")
}

func unzip(dir string, r io.Reader, stripPathComponents int) error {
	// zips require ReaderAt, most installers are quite small so we'll just buffer them in memory
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return errors.Wrap(err, "error reading zip file")
	}

	readerAt := bytes.NewReader(buf.Bytes())

	zf, zerr := zip.NewReader(readerAt, int64(buf.Len()))
	if zerr != nil {
		return errors.Wrap(zerr, "unable to open zip")
	}

	for _, f := range zf.File {
		fpath, ok := joinAndStripPath(dir, f.Name, stripPathComponents)
		if !ok {
			continue
		}

		if err := os.MkdirAll(filepath.Dir(fpath), dirMode); err != nil {
			return errors.Wrap(err, "error creating parent directory")
		}

		switch f.FileInfo().Mode() & os.ModeType {
		case os.ModeDir:
			if err := os.MkdirAll(fpath, dirMode); err != nil {
				return errors.Wrap(err, "error creating directory")
			}

			continue

		case 0:
			fc, err := f.Open()
			if err != nil {
				return errors.Wrap(err, "error opening zip entry")
			}

			if ferr := createFile(fpath, f.FileInfo().Mode(), f.FileInfo().ModTime(), fc); ferr != nil {
				return errors.Wrapf(ferr, "error creating file %v", f.Name)
			}

			fc.Close() //nolint:errcheck

		default:
			return errors.Errorf("unsupported zip entry %v: %v", f.Name, f.FileInfo().Mode())
		}
	}

	return nil
}

// Download downloads the provided URL and extracts it to the provided directory, retrying
// exponentionally until succeeded.
func Download(url, dir string, checksum map[string]string, stripPathComponents int) error {
	const (
		// sleep durations 5, 10, 20, 40, 80, 160, 320
		// total: 635 seconds, ~10 minutes
		maxRetries       = 8
		initialSleepTime = 5 * time.Second
	)

	nextSleepTime := initialSleepTime

	for i := range maxRetries {
		err := downloadInternal(url, dir, checksum, stripPathComponents)
		if err == nil {
			// success
			return nil
		}

		// 404 is non-retryable
		if errors.Is(err, errNotFound) {
			return errors.Wrap(err, "non-retryable")
		}

		// invalid checksum is non-retryable
		var ec InvalidChecksumError
		if errors.As(err, &ec) {
			// invalid checksum, do not retry.
			return errors.Wrap(err, "non-retryable")
		}

		// all other errors are retryable
		if i != maxRetries-1 {
			log.Printf("Attempt #%v failed, sleeping for %v: %v", i, nextSleepTime, err)
			time.Sleep(nextSleepTime)

			nextSleepTime *= 2

			if err := os.RemoveAll(dir); err != nil {
				log.Printf("unable to remove %v: %v", dir, err)
			}
		}
	}

	return errors.Errorf("unable to download %v", url)
}

// InvalidChecksumError is returned by Download when the checksum of the downloaded file does not match the expected checksum.
type InvalidChecksumError struct {
	actual   string
	expected string
}

func (e InvalidChecksumError) Error() string {
	if e.expected == "" {
		return fmt.Sprintf("missing checksum: %v", e.actual)
	}

	return fmt.Sprintf("invalid checksum: %v, wanted %v", e.actual, e.expected)
}

var errNotFound = errors.New("not found")

func downloadInternal(url, dir string, checksum map[string]string, stripPathComponents int) (err error) {
	resp, err := http.Get(url) //nolint:gosec,noctx
	if err != nil {
		return errors.Wrapf(err, "unable to get %q", url)
	}

	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return errNotFound
		}

		return errors.Errorf("invalid server response for %q: %v", url, resp.Status)
	}

	var buf bytes.Buffer

	h := sha256.New()

	if _, cerr := io.Copy(io.MultiWriter(h, &buf), resp.Body); cerr != nil {
		return errors.Wrap(cerr, "copy error")
	}

	actualChecksum := hex.EncodeToString(h.Sum(nil))

	switch {
	case checksum[url] == "":
		checksum[url] = actualChecksum
		return InvalidChecksumError{actualChecksum, ""}

	case checksum[url] != actualChecksum:
		return InvalidChecksumError{actualChecksum, checksum[url]}

	default:
		log.Printf("%v checksum ok", url)
	}

	var r io.Reader

	if strings.HasSuffix(url, ".gz") {
		gzr, err := gzip.NewReader(&buf)
		if err != nil {
			return errors.New("unable to gunzip response")
		}

		r = gzr
	} else {
		r = &buf
	}

	switch {
	case strings.HasSuffix(url, ".tar.gz"):
		return errors.Wrap(untar(dir, r, stripPathComponents), "untar error")
	case strings.HasSuffix(url, ".zip"):
		return errors.Wrap(unzip(dir, r, stripPathComponents), "unzip error")
	default:
		return errors.New("unsupported archive format")
	}
}
