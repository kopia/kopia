// Package storj implements Storage based on Storj distributed storage system.
package storj

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"storj.io/uplink"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	storjStorageType = "storj"

	// TODO(rjk): I'm not certain that I'm correctly handling modification times.
	timeMapKey = "Kopia-Mtime" // case is important, first letter must be capitalized.
)

type storjStorage struct {
	Options
	blob.DefaultProviderImplementation
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

// GetBlob returns full or partial contents of a blob with given ID.
func (storj *storjStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	if offset < 0 {
		return blob.ErrInvalidRange
	}
	key := storj.getObjectNameString(b)
	opts := uplink.DownloadOptions{
		Offset: offset,
		Length: length,
	}
	dl, err := storj.project.DownloadObject(ctx, storj.BucketName, key, &opts)
	if err != nil {
		return errors.Wrap(translateError(err), "GetBlob DownloadObject")
	}

	if err := iocopy.JustCopy(output, dl); err != nil {
		return errors.Wrap(translateError(err), "GetBlob JustCopy")
	}

	//nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func (storj *storjStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	key := storj.getObjectNameString(b)

	o, err := storj.project.StatObject(ctx, storj.BucketName, key)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(translateError(err), "GetMetadata StatObject fail")
	}
	bmd := blob.Metadata{
		BlobID:    blob.ID(o.Key),
		Length:    o.System.ContentLength,
		Timestamp: o.System.Created,
	}
	return bmd, nil
}

func (storj *storjStorage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	switch {
	case opts.RetentionMode != "":
		return blob.ErrUnsupportedPutBlobOption
	case !opts.SetModTime.IsZero():
		// TODO(rjk): This is inspired by the S3 handling.
		return blob.ErrSetTimeUnsupported
	}

	pb := storj.getObjectNameString(b)
	if opts.DoNotRecreate {
		if _, err := storj.project.StatObject(ctx, storj.BucketName, pb); err == nil || !errors.Is(err, uplink.ErrObjectNotFound) {
			return errors.Wrap(blob.ErrBlobAlreadyExists, "PutBob, DoNotRecreate")
		}
	}

	// TODO(rjk): Implement multi-part uploads for segmented blob.Bytes in a
	// subsequent PR.
	upfd, err := storj.project.UploadObject(ctx, storj.BucketName, pb, nil)
	if err != nil {
		return errors.Wrap(err, "PutBlob UploadObject")
	}

	if err := iocopy.JustCopy(upfd, data.Reader()); err != nil {
		return errors.Wrap(err, "PutBlob copying")
	}

	if err := upfd.Commit(); err != nil {
		return errors.Wrap(err, "PubBlob Commit")
	}

	// TODO(rjk): I am not certain that I am correctly handling modifcation times.
	if opts.GetModTime != nil {
		info := upfd.Info()
		*opts.GetModTime = info.System.Created
	}
	return nil
}

func (storj *storjStorage) DeleteBlob(ctx context.Context, b blob.ID) error {
	key := storj.getObjectNameString(b)
	_, err := storj.project.DeleteObject(ctx, storj.BucketName, key)
	return errors.Wrap(translateError(err), "DeleteBlob")
}

func (storj *storjStorage) getObjectNameString(blobID blob.ID) string {
	return storj.Prefix + string(blobID)
}

func (storj *storjStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	oi := storj.project.ListObjects(ctx, storj.Options.BucketName, &uplink.ListObjectsOptions{
		System: true,
	})
	sp := storj.getObjectNameString(prefix)

	for {
		if !oi.Next() {
			return errors.Wrap(oi.Err(), "ListBlobs iteration")
		}

		o := oi.Item()
		// TODO(rjk): This should use sharding to have prefix optimization?
		if !strings.HasPrefix(o.Key, sp) {
			continue
		}
		omd := blob.Metadata{
			BlobID:    blob.ID(o.Key),
			Length:    o.System.ContentLength,
			Timestamp: o.System.Created,
		}

		if err := callback(omd); err != nil {
			return errors.Wrap(err, "ListBlobs iteration callback had an error")
		}
	}
}

func (storj *storjStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   storjStorageType,
		Config: &storj.Options,
	}
}

func (storj *storjStorage) DisplayName() string {
	return fmt.Sprintf("Storj: %v", storj.BucketName)
}

func (storj *storjStorage) Close(ctx context.Context) error {
	return errors.Wrap(storj.project.Close(), "error closing Storj project")
}

// Code is based on the usage flow documented at
// https://github.com/storj/storj/wiki/Libuplink-Walkthrough
// TODO(rjk): What does "isCreate" do?
func _new(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, *storjStorage, error) {
	access, err := uplink.ParseAccess(opt.AccessGrant)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not parse access grant")
	}

	// Open up the Project we will be working with.
	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not open project")
	}

	// Ensure the desired Bucket within the Project is created.
	_, err = project.EnsureBucket(ctx, opt.BucketName)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not ensure bucket")
	}

	storj := &storjStorage{
		Options: *opt,
		project: project,
	}
	return retrying.NewWrapper(storj), storj, nil
}

// New creates new Storj-backed storage with specified options.
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
	s, _, err := _new(ctx, opt, isCreate)
	return s, err
}

func init() {
	blob.AddSupportedStorage(storjStorageType, Options{}, New)
}

// Make sure that I implemented everything.
var _ blob.Storage = &storjStorage{}
