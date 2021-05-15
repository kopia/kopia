// Package webdav implements WebDAV-based Storage.
package webdav

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/studio-b12/gowebdav"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/internal/tlsutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
	"github.com/kopia/kopia/repo/blob/sharded"
)

const (
	davStorageType       = "webdav"
	fsStorageChunkSuffix = ".f"

	defaultFilePerm = 0o600
	defaultDirPerm  = 0o700
)

var fsDefaultShards = []int{3, 3}

// davStorage implements blob.Storage on top of remove WebDAV repository.
// It is very similar to File storage, except uses HTTP URLs instead of local files.
// Storage formats are compatible (both use sharded directory structure), so a repository
// may be accessed using WebDAV or File interchangeably.
type davStorage struct {
	sharded.Storage
}

type davStorageImpl struct {
	Options

	cli *gowebdav.Client
}

func (d *davStorageImpl) GetBlobFromPath(ctx context.Context, dirPath, path string, offset, length int64) ([]byte, error) {
	data, err := d.cli.Read(path)
	if err != nil {
		return nil, d.translateError(err)
	}

	if int(offset) > len(data) || offset < 0 {
		return nil, errors.Wrap(blob.ErrInvalidRange, "invalid offset")
	}

	// nolint:wrapcheck
	return blob.EnsureLengthAndTruncate(data[offset:], length)
}

func (d *davStorageImpl) GetMetadataFromPath(ctx context.Context, dirPath, path string) (blob.Metadata, error) {
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

func (d *davStorageImpl) PutBlobInPath(ctx context.Context, dirPath, filePath string, data blob.Bytes) error {
	tmpPath := fmt.Sprintf("%v-%v", filePath, rand.Int63()) //nolint:gosec

	var buf bytes.Buffer

	data.WriteTo(&buf) // nolint:errcheck

	b := buf.Bytes()

	// nolint:wrapcheck
	return retry.WithExponentialBackoffNoValue(ctx, "WriteTemporaryFileAndCreateParentDirs", func() error {
		mkdirAttempted := false

		for {
			// nolint:wrapcheck
			err := d.translateError(d.cli.Write(tmpPath, b, defaultFilePerm))
			if err == nil {
				// nolint:wrapcheck
				return d.cli.Rename(tmpPath, filePath, true)
			}

			// An error above may indicate that the directory doesn't exist.
			// Attempt to create required directories and try again, if successful.
			if !mkdirAttempted {
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
	}, isRetriable)
}

func (d *davStorageImpl) SetTimeInPath(ctx context.Context, dirPath, filePath string, n time.Time) error {
	return blob.ErrSetTimeUnsupported
}

func (d *davStorageImpl) DeleteBlobInPath(ctx context.Context, dirPath, filePath string) error {
	return d.translateError(retry.WithExponentialBackoffNoValue(ctx, "DeleteBlobInPath", func() error {
		// nolint:wrapcheck
		return d.cli.Remove(filePath)
	}, isRetriable))
}

func (d *davStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   davStorageType,
		Config: &d.Storage.Impl.(*davStorageImpl).Options,
	}
}

func (d *davStorage) DisplayName() string {
	o := d.Storage.Impl.(*davStorageImpl).Options
	return fmt.Sprintf("WebDAV: %v", o.URL)
}

func (d *davStorage) Close(ctx context.Context) error {
	return nil
}

func isRetriable(err error) bool {
	var pe *os.PathError

	switch {
	case err == nil:
		return false

	case errors.As(err, &pe):
		httpCode := httpErrorCode(pe)
		return httpCode == 429 || httpCode >= 500

	default:
		return true
	}
}

// New creates new WebDAV-backed storage in a specified URL.
func New(ctx context.Context, opts *Options) (blob.Storage, error) {
	cli := gowebdav.NewClient(opts.URL, opts.Username, opts.Password)

	if opts.TrustedServerCertificateFingerprint != "" {
		cli.SetTransport(tlsutil.TransportTrustingSingleCertificate(opts.TrustedServerCertificateFingerprint))
	}

	s := retrying.NewWrapper(&davStorage{
		sharded.Storage{
			Impl: &davStorageImpl{
				Options: *opts,
				cli:     cli,
			},
			RootPath: "",
			Suffix:   fsStorageChunkSuffix,
			Shards:   opts.shards(),
		},
	})

	return s, nil
}

func init() {
	blob.AddSupportedStorage(
		davStorageType,
		func() interface{} { return &Options{} },
		func(ctx context.Context, o interface{}) (blob.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
