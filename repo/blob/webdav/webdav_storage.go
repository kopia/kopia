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

	"github.com/pkg/errors"
	"github.com/studio-b12/gowebdav"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
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
	v, err := retry.WithExponentialBackoff(ctx, "GetBlobFromPath", func() (interface{}, error) {
		return d.cli.Read(path)
	}, isRetriable)
	if err != nil {
		return nil, d.translateError(err)
	}

	data := v.([]byte)

	if length < 0 {
		return data, nil
	}

	if int(offset) > len(data) || offset < 0 {
		return nil, errors.New("invalid offset")
	}

	data = data[offset:]
	if int(length) > len(data) {
		return nil, errors.New("invalid length")
	}

	return data[0:length], nil
}

func (d *davStorageImpl) GetMetadataFromPath(ctx context.Context, dirPath, path string) (blob.Metadata, error) {
	v, err := retry.WithExponentialBackoff(ctx, "GetMetadataFromPath", func() (interface{}, error) {
		return d.cli.Stat(path)
	}, isRetriable)
	if err != nil {
		return blob.Metadata{}, d.translateError(err)
	}

	fi := v.(os.FileInfo)

	return blob.Metadata{
		Length:    fi.Size(),
		Timestamp: fi.ModTime(),
	}, nil
}

func httpErrorCode(err error) int {
	if err, ok := err.(*os.PathError); ok {
		code, err := strconv.Atoi(strings.Split(err.Err.Error(), " ")[0])
		if err == nil {
			return code
		}
	}

	return 0
}

func (d *davStorageImpl) translateError(err error) error {
	switch err := err.(type) {
	case *os.PathError:
		switch httpErrorCode(err) {
		case http.StatusNotFound:
			return blob.ErrBlobNotFound
		default:
			return err
		}
	default:
		return err
	}
}

func (d *davStorageImpl) ReadDir(ctx context.Context, dir string) ([]os.FileInfo, error) {
	v, err := retry.WithExponentialBackoff(ctx, "ReadDir("+dir+")", func() (interface{}, error) {
		return d.cli.ReadDir(gowebdav.FixSlash(dir))
	}, isRetriable)
	if err == nil {
		return v.([]os.FileInfo), nil
	}

	return nil, err
}

func (d *davStorageImpl) PutBlobInPath(ctx context.Context, dirPath, filePath string, data blob.Bytes) error {
	tmpPath := fmt.Sprintf("%v-%v", filePath, rand.Int63()) //nolint:gosec

	var buf bytes.Buffer

	data.WriteTo(&buf) // nolint:errcheck

	b := buf.Bytes()

	if err := d.translateError(retry.WithExponentialBackoffNoValue(ctx, "Write", func() error {
		return d.cli.Write(tmpPath, b, defaultFilePerm)
	}, isRetriable)); err != nil {
		if !errors.Is(err, blob.ErrBlobNotFound) {
			return err
		}

		_ = retry.WithExponentialBackoffNoValue(ctx, "MkdirAll", func() error {
			return d.cli.MkdirAll(dirPath, defaultDirPerm)
		}, isRetriable)

		if err := d.translateError(retry.WithExponentialBackoffNoValue(ctx, "Write", func() error {
			return d.cli.Write(tmpPath, b, defaultFilePerm)
		}, isRetriable)); err != nil {
			return err
		}
	}

	return d.translateError(d.cli.Rename(tmpPath, filePath, true))
}

func (d *davStorageImpl) DeleteBlobInPath(ctx context.Context, dirPath, filePath string) error {
	return d.translateError(retry.WithExponentialBackoffNoValue(ctx, "DeleteBlobInPath", func() error {
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
	switch err := err.(type) {
	case nil:
		return false

	case *os.PathError:
		httpCode := httpErrorCode(err)
		return httpCode == 429 || httpCode >= 500

	default:
		return true
	}
}

// New creates new WebDAV-backed storage in a specified URL.
func New(ctx context.Context, opts *Options) (blob.Storage, error) {
	return &davStorage{
		sharded.Storage{
			Impl: &davStorageImpl{
				Options: *opts,
				cli:     gowebdav.NewClient(opts.URL, opts.Username, opts.Password),
			},
			RootPath: "",
			Suffix:   fsStorageChunkSuffix,
			Shards:   opts.shards(),
		},
	}, nil
}

func init() {
	blob.AddSupportedStorage(
		davStorageType,
		func() interface{} { return &Options{} },
		func(ctx context.Context, o interface{}) (blob.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
