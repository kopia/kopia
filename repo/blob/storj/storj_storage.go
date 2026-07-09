// Package storj implements Storage based on Storj distributed storage system.
package storj

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pkg/errors"
	"storj.io/uplink"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sharded"
)

const (
	storjStorageType = "storj"

	// TODO(rjk): I'm not certain that I'm correctly handling modification times.
	timeMapKey = "Kopia-Mtime" // case is important, first letter must be capitalized.
)

type storjStorage struct {
	sharded.Storage
	blob.DefaultProviderImplementation
}

type impl struct {
	Options
	project *uplink.Project
}

func translateError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, uplink.ErrObjectNotFound):
		return blob.ErrBlobNotFound
	case errors.Is(err, uplink.ErrObjectKeyInvalid):
		return blob.ErrBlobNotFound
	case errors.Is(err, uplink.ErrPermissionDenied):
		return blob.ErrInvalidCredentials
	default:
		return errors.Wrap(err, "Storj error")
	}
}

func (storj *impl) MakeFileName(blobID blob.ID) string {
	return string(blobID)
}
func (storj *impl) GetBlobIDFromFileName(name string) (blob.ID, bool) {
	return blob.ID(name), true
}

func (storj *impl) _getBlobFromPath(ctx context.Context, key string, offset, length int64, output blob.OutputBuffer) error {
	if offset < 0 {
		return blob.ErrInvalidRange
	}
	opts := uplink.DownloadOptions{
		Offset: offset,
		Length: length,
	}
	dl, err := storj.project.DownloadObject(ctx, storj.BucketName, key, &opts)
	if err != nil {
		return errors.Wrap(translateError(err), "GetBlobFromPath")
	}
	// TODO(rjk): Is this erroring a fatal?
	defer dl.Close()

	// TODO(rjk): Why is this necessary? I would *not* have expected this but
	// validate-provider requires this. Without this line, _getBlobFromPath
	// writes to the end of output.
	output.Reset()

	if err := iocopy.JustCopy(output, dl); err != nil {
		return errors.Wrap(translateError(err), "GetBlob JustCopy")
	}

	//nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func (storj *impl) GetBlobFromPath(ctx context.Context, dirPath, path string, offset, length int64, output blob.OutputBuffer) error {
	return retry.WithExponentialBackoffNoValue(ctx, "GetBlobFromPath("+path+")", func() error {
		return storj._getBlobFromPath(ctx, path, offset, length, output)
	}, isRetriable)
}

func (storj *impl) _getMetadataFromPath(ctx context.Context, key string) (blob.Metadata, error) {
	o, err := storj.project.StatObject(ctx, storj.BucketName, key)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(translateError(err), "_getMetadataFromPath StatObject fail")
	}
	bmd := blob.Metadata{
		BlobID:    blob.ID(o.Key),
		Length:    o.System.ContentLength,
		Timestamp: o.System.Created,
	}
	return bmd, nil
}

func (storj *impl) GetMetadataFromPath(ctx context.Context, dirPath, filePath string) (blob.Metadata, error) {
	return retry.WithExponentialBackoff(ctx, "GetMetadataFromPath("+filePath+")", func() (blob.Metadata, error) {
		return storj._getMetadataFromPath(ctx, filePath)
	}, isRetriable)
}

func (storj *impl) _putBlob(ctx context.Context, key string, dataSlices blob.Bytes, opts blob.PutOptions) error {
	switch {
	case opts.RetentionMode != "":
		return blob.ErrUnsupportedPutBlobOption
	case !opts.SetModTime.IsZero():
		// TODO(rjk): This is inspired by the S3 handling.
		return blob.ErrSetTimeUnsupported
	}

	if opts.DoNotRecreate {
		if _, err := storj.project.StatObject(ctx, storj.BucketName, key); err == nil || !errors.Is(err, uplink.ErrObjectNotFound) {
			return errors.Wrap(blob.ErrBlobAlreadyExists, "_putBlob, DoNotRecreate")
		}
	}

	// TODO(rjk): Implement multi-part uploads for segmented blob.Bytes in a
	// subsequent PR.
	upfd, err := storj.project.UploadObject(ctx, storj.BucketName, key, nil)
	if err != nil {
		return errors.Wrap(err, "_putBlob UploadObject")
	}

	if err := iocopy.JustCopy(upfd, dataSlices.Reader()); err != nil {
		return errors.Wrap(err, "_putBlob copying")
	}

	if err := upfd.Commit(); err != nil {
		return errors.Wrap(err, "_putBlob Commit")
	}

	// TODO(rjk): I am not certain that I am correctly handling modifcation times.
	if opts.GetModTime != nil {
		info := upfd.Info()
		*opts.GetModTime = info.System.Created
	}
	return nil
}

func (s impl) PutBlobInPath(ctx context.Context, dirPath, filePath string, dataSlices blob.Bytes, opts blob.PutOptions) error {
	return retry.WithExponentialBackoffNoValue(ctx, "PutBlobInPath("+filePath+")", func() error {
		//nolint:wrapcheck
		return s._putBlob(ctx, filePath, dataSlices, opts)
	}, isRetriable)
}

func (storj *impl) _deleteBlobInPath(ctx context.Context, key string) error {
	_, err := storj.project.DeleteObject(ctx, storj.BucketName, key)
	return errors.Wrap(translateError(err), "DeleteBlob")
}

func (s *impl) DeleteBlobInPath(ctx context.Context, dirPath, filePath string) error {
	//nolint:wrapcheck
	return retry.WithExponentialBackoffNoValue(ctx, "DeleteBlob("+filePath+")", func() error {
		return s._deleteBlobInPath(ctx, filePath)
	}, isRetriable)
}

// Use uplink.Object instances as os.FileInfo.
type storjfileinfo uplink.Object

func (e *storjfileinfo) Name() string {
	// Storj prefix listings include the full path but ReadDir expects just
	// the the Base.
	return path.Base(e.Key)
}
func (e *storjfileinfo) Size() int64 {
	return e.System.ContentLength
}
func (e *storjfileinfo) Mode() os.FileMode {
	if e.IsPrefix {
		return os.ModeDir
	} else {
		return 0
	}
}
func (e *storjfileinfo) ModTime() time.Time {
	return e.System.Created
}
func (e *storjfileinfo) IsDir() bool {
	return e.IsPrefix
}
func (e *storjfileinfo) Sys() any {
	return nil
}

func (s *impl) ReadDir(ctx context.Context, path string) ([]os.FileInfo, error) {
	listing := []os.FileInfo{}

	if strings.HasPrefix(path, "/") {
		path = path[1:]
	}
	if path != "" {
		path = path + "/"
	}

	oi := s.project.ListObjects(ctx, s.Options.BucketName, &uplink.ListObjectsOptions{
		System: true,
		Prefix: path,
	})

	for {
		if !oi.Next() {
			return listing, errors.Wrap(oi.Err(), "ReadDir iteration")
		}
		listing = append(listing, (*storjfileinfo)(oi.Item()))
	}
}

func (storj *storjStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   storjStorageType,
		Config: &storj.Storage.Impl.(*impl).Options, //nolint:forcetypeassert
	}
}

func (storj *storjStorage) DisplayName() string {
	return fmt.Sprintf("Storj: %v", storj.Storage.Impl.(*impl).BucketName)
}

func (storj *storjStorage) Close(ctx context.Context) error {
	return errors.Wrap(storj.Storage.Impl.(*impl).project.Close(), "error closing Storj project")
}

// Code is based on the usage flow documented at
// https://github.com/storj/storj/wiki/Libuplink-Walkthrough
func _new(ctx context.Context, opt *Options, isCreate bool) (*storjStorage, error) {
	// Each concurrent Storj download fans out to ~80 storage-node TCP connections
	// (erasure coding). Without a concurrency cap the ephemeral port range is
	// exhausted during maintenance on large repos, causing EADDRNOTAVAIL.
	if opt.ConcurrentReads == 0 {
		optCopy := *opt
		optCopy.ConcurrentReads = 4
		opt = &optCopy
	}

	access, err := uplink.ParseAccess(opt.AccessGrant)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse access grant")
	}

	// Open up the Project we will be working with.
	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		return nil, errors.Wrap(err, "could not open project")
	}

	// Ensure the desired Bucket within the Project is created.
	_, err = project.EnsureBucket(ctx, opt.BucketName)
	if err != nil {
		return nil, errors.Wrap(err, "could not ensure bucket")
	}

	si := &impl{
		Options: *opt,
		project: project,
	}

	storj := &storjStorage{
		Storage: sharded.New(si, opt.Prefix, opt.Options, isCreate),
	}

	return storj, err
}

// New creates new Storj-backed storage with specified options.
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
	return _new(ctx, opt, isCreate)
}

func init() {
	blob.AddSupportedStorage(storjStorageType, Options{}, New)
}

// Make sure that I implemented everything.
var _ blob.Storage = &storjStorage{}

func isRetriable(err error) bool {
	switch {
	case errors.Is(err, blob.ErrBlobNotFound):
		return false

	case errors.Is(err, blob.ErrInvalidRange):
		return false

	case errors.Is(err, blob.ErrSetTimeUnsupported):
		return false

	case errors.Is(err, blob.ErrInvalidCredentials):
		return false

	case errors.Is(err, blob.ErrUnsupportedPutBlobOption):
		return false

	case errors.Is(err, blob.ErrBlobAlreadyExists):
		return false

	default:
		return true
	}
}
