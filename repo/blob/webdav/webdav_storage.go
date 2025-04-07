// Package webdav implements WebDAV-based Storage.
package webdav

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/studio-b12/gowebdav"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/internal/tlsutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
	"github.com/kopia/kopia/repo/blob/sharded"
)

const (
	davStorageType = "webdav"

	defaultFilePerm = 0o600
	defaultDirPerm  = 0o700
)

// davStorage implements blob.Storage on top of remove WebDAV repository.
// It is very similar to File storage, except uses HTTP URLs instead of local files.
// Storage formats are compatible (both use sharded directory structure), so a repository
// may be accessed using WebDAV or File interchangeably.
type davStorage struct {
	sharded.Storage
	blob.DefaultProviderImplementation
}

type davStorageImpl struct {
	Options

	cli *gowebdav.Client
}

func (d *davStorageImpl) GetBlobFromPath(ctx context.Context, dirPath, path string, offset, length int64, output blob.OutputBuffer) error {
	_ = dirPath

	output.Reset()

	if offset < 0 {
		return blob.ErrInvalidRange
	}

	var (
		s   io.ReadCloser
		err error
	)

	switch {
	case length < 0:
		s, err = d.cli.ReadStream(path)
	case length == 0:
		s, err = d.cli.ReadStreamRange(path, offset, 1)
	default:
		s, err = d.cli.ReadStreamRange(path, offset, length)
	}

	if err != nil {
		return d.translateError(err)
	}

	defer s.Close() //nolint:errcheck

	if length == 0 {
		return nil
	}

	if err := iocopy.JustCopy(output, s); err != nil {
		return errors.Wrap(err, "error populating output")
	}

	//nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func (d *davStorageImpl) GetMetadataFromPath(ctx context.Context, dirPath, path string) (blob.Metadata, error) {
	_ = dirPath

	fi, err := d.cli.Stat(path)
	if err != nil {
		return blob.Metadata{}, d.translateError(err)
	}

	return blob.Metadata{
		Length:    fi.Size(),
		Timestamp: fi.ModTime(),
	}, nil
}

func httpErrorCode(err error) int {
	var pe *os.PathError

	if errors.As(err, &pe) {
		code, err := strconv.Atoi(strings.Split(pe.Err.Error(), " ")[0])
		if err == nil {
			return code
		}
	}

	return 0
}

func (d *davStorageImpl) translateError(err error) error {
	var pe *os.PathError

	if errors.As(err, &pe) {
		switch httpErrorCode(pe) {
		case http.StatusRequestedRangeNotSatisfiable:
			return blob.ErrInvalidRange

		case http.StatusNotFound:
			return blob.ErrBlobNotFound
		}
	}

	return err
}

func (d *davStorageImpl) ReadDir(ctx context.Context, dir string) ([]os.FileInfo, error) {
	entries, err := d.cli.ReadDir(gowebdav.FixSlash(dir))
	if err == nil {
		return entries, nil
	}

	return nil, errors.Wrap(err, "error reading WebDAV dir")
}

func (d *davStorageImpl) PutBlobInPath(ctx context.Context, dirPath, filePath string, data blob.Bytes, opts blob.PutOptions) error {
	switch {
	case opts.HasRetentionOptions():
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "blob-retention")
	case opts.DoNotRecreate:
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "do-not-recreate")
	}

	if !opts.SetModTime.IsZero() {
		return blob.ErrSetTimeUnsupported
	}

	var writePath string

	if d.Options.AtomicWrites {
		writePath = filePath
	} else {
		writePath = fmt.Sprintf("%v-%v", filePath, rand.Int63()) //nolint:gosec
	}

	var buf bytes.Buffer

	data.WriteTo(&buf) //nolint:errcheck

	b := buf.Bytes()

	if err := retry.WithExponentialBackoffNoValue(ctx, "WriteTemporaryFileAndCreateParentDirs", func() error {
		mkdirAttempted := false

		for {

			err := d.translateError(d.cli.Write(writePath, b, defaultFilePerm))
			if err == nil {
				if d.Options.AtomicWrites {
					return nil
				}

				return d.cli.Rename(writePath, filePath, true)
			}

			// An error above may indicate that the directory doesn't exist.
			// Attempt to create required directories and try again, if successful.
			if !mkdirAttempted && dirPath != "" {
				mkdirAttempted = true

				if mkdirErr := d.cli.MkdirAll(dirPath, defaultDirPerm); mkdirErr == nil {
					// If MkdirAll succeeds, the missing directory was likely
					// the problem, so try again immediately.
					//
					// Otherwise, fall through and return the original error.
					continue
				}
			}

			return err
		}
	}, isRetriable); err != nil {
		return err
	}

	if opts.GetModTime != nil {
		bm, err := d.GetMetadataFromPath(ctx, dirPath, filePath)
		if err != nil {
			return err
		}

		*opts.GetModTime = bm.Timestamp
	}

	return nil
}

func (d *davStorageImpl) DeleteBlobInPath(ctx context.Context, dirPath, filePath string) error {
	_ = dirPath

	err := d.translateError(retry.WithExponentialBackoffNoValue(ctx, "DeleteBlobInPath", func() error {
		return d.cli.Remove(filePath)
	}, isRetriable))
	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}

	return err
}

func (d *davStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   davStorageType,
		Config: &d.Storage.Impl.(*davStorageImpl).Options, //nolint:forcetypeassert
	}
}

func (d *davStorage) DisplayName() string {
	o := d.Storage.Impl.(*davStorageImpl).Options //nolint:forcetypeassert
	return fmt.Sprintf("WebDAV: %v", o.URL)
}

func isRetriable(err error) bool {
	var pe *os.PathError

	switch {
	case err == nil:
		return false

	case errors.As(err, &pe):
		httpCode := httpErrorCode(pe)
		switch httpCode {
		case http.StatusLocked, http.StatusConflict, http.StatusTooManyRequests:
			return true

		default:
			return httpCode >= http.StatusInternalServerError
		}

	default:
		return true
	}
}

// New creates new WebDAV-backed storage in a specified URL.
func New(ctx context.Context, opts *Options, isCreate bool) (blob.Storage, error) {
	cli := gowebdav.NewClient(opts.URL, opts.Username, opts.Password)

	// Since we're handling encrypted data, there's no point compressing it server-side.
	cli.SetHeader("Accept-Encoding", "identity")

	if opts.TrustedServerCertificateFingerprint != "" {
		cli.SetTransport(tlsutil.TransportTrustingSingleCertificate(opts.TrustedServerCertificateFingerprint))
	}

	s := retrying.NewWrapper(&davStorage{
		Storage: sharded.New(&davStorageImpl{
			Options: *opts,
			cli:     cli,
		}, "", opts.Options, isCreate),
	})

	return s, nil
}

func init() {
	blob.AddSupportedStorage(davStorageType, Options{}, New)
}
