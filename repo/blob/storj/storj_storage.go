package storj

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"storj.io/uplink"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	storjStorageType = "storj"
	storjSchemePfx   = "sj://"
)

// ErrIncompleteIO is returned when bytes read or written do not equal the buffer length.
var ErrIncompleteIO = errors.New("bytes read/written != buffer length")

var (
	errNilProject        = errors.New("refusing to close nil project")
	errMissingBucketName = errors.New("missing bucket name")
	errDoNotRecreate     = errors.New("do-not-recreate")
	errStorageIsNil      = errors.New("storage is nil after initialization")
	errNotImplemented    = errors.New("not yet implemented")
)

// TODO: remove if unused
// type storjPointInTimeStorage struct {
// 	StorjStorage
// 	pointInTime time.Time
// }

// StorjStorage implements blob.Storage for Storj decentralized cloud storage.
//
//nolint:revive
type StorjStorage struct {
	blob.Storage // why does s3 not (need to) encapsulate this???
	Options
	project       *uplink.Project
	encrypted     bool //nolint:unused // TODO: how to use?
	storageConfig *StorageConfig
}

// enriched uplink.CustomMetadata for convenient conversions to known field types.
type customMeta uplink.CustomMetadata

func newCustomMeta() (cm customMeta) {
	cm = make(customMeta)
	cm["ContentType"] = "application/x-kopia"
	cm.SetLastModified(clock.Now()) // assumes instantiation of customMeta is "close enough" to object creation

	return cm
}

func (c customMeta) GetLastModified() time.Time {
	var t time.Time
	if lastmod, ok := c["LastModified"]; ok {
		t, _ = time.Parse(time.RFC3339, lastmod) // TODO: check whether non-nil error indeed gives the time zero value!
	}

	return t
}

func (c customMeta) GetLastModifiedOrDefault(dflt time.Time) (ts time.Time) {
	ts = c.GetLastModified()
	if ts.IsZero() {
		return dflt
	}

	return ts
}

func (c customMeta) SetLastModified(ts time.Time) {
	c["LastModified"] = ts.Format(time.RFC3339)
}

// New creates a new Storj blob storage backend, optionally creating the bucket.
func New(ctx context.Context, opt *Options, createBucket bool) (blob.Storage, error) {
	st, err := newStorage(ctx, opt, createBucket)
	if err != nil {
		return nil, err
	}

	if st == nil {
		return nil, errStorageIsNil
	}

	return retrying.NewWrapper(st), nil
}

// NewUnwrapped creates a new Storj blob storage without retry wrapping.
func NewUnwrapped(ctx context.Context, opt *Options, createBucket bool) (blob.Storage, error) {
	st, err := newStorage(ctx, opt, createBucket)
	if err != nil {
		return nil, err
	}

	if st == nil {
		return nil, errStorageIsNil
	}

	return st, nil
}

// Close implements blob.Storage.
func (s *StorjStorage) Close(_ context.Context) error {
	if s.project == nil {
		return errNilProject
	}

	if err := s.project.Close(); err != nil {
		return fmt.Errorf("closing project: %w", err)
	}

	return nil
}

func newStorage(ctx context.Context, opt *Options, isCreate bool) (storjStorage *StorjStorage, err error) {
	storjStorage = &StorjStorage{
		Options: *opt,
		// storjExt:      stext,
		storageConfig: &StorageConfig{},
	}

	proj, err := storjStorage.GetProject(ctx)
	if err != nil {
		return nil, err
	}

	// should we be interested in the bucket instance itself? (convenient to use as member of storage maybe? It only has some metadata)
	if isCreate {
		if _, err = proj.EnsureBucket(ctx, opt.BucketName); err != nil {
			return nil, fmt.Errorf("ensuring bucket %q: %w", opt.BucketName, err)
		}
	} else {
		if _, err = proj.StatBucket(ctx, opt.BucketName); err != nil {
			return nil, fmt.Errorf("checking bucket %q: %w", opt.BucketName, err)
		}
	}

	return storjStorage, nil
}

// maybePointInTimeStore wraps s with a point-in-time store when s is versioned
// and a point-in-time value is specified. Otherwise s is returned.
// TODO: check whether/how this is implemented in storj and remove if not
// func maybePointInTimeStore(ctx context.Context, s *StorjStorage, pointInTime *time.Time) (blob.Storage, error) {
// 	if pit := s.Options.PointInTime; pit == nil || pit.IsZero() {
// 		return s, nil
// 	}
//
// 	return readonly.NewWrapper(&storjPointInTimeStorage{
// 		StorjStorage: *s,
// 		pointInTime:  *pointInTime,
// 	}), nil
// }

// GetProject returns the uplink Project, opening it if not already cached.
func (s *StorjStorage) GetProject(ctx context.Context) (project *uplink.Project, err error) {
	if s.project != nil {
		return s.project, nil
	}
	// TODO: remove the API key access method from options/docs
	access, err := uplink.ParseAccess(s.KeyOrGrant)
	if err != nil {
		return nil, fmt.Errorf("could not request access grant: %w", err)
	}

	project, err = uplink.OpenProject(ctx, access)
	if err != nil {
		return nil, fmt.Errorf("could not open project: %w", err)
	}

	s.project = project
	// FIXME: where do we do project.Close() -> actually the Storage interface has Close()!
	// we only don't know where/whether kopia calls it, because e.g. S3 doesn't even implement it
	return project, nil
}

// PutBlob implements blob.Storage.
func (s *StorjStorage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	if s.BucketName == "" {
		return errors.Join(blob.ErrNotAVolume, errMissingBucketName)
	}

	if _, err := s.GetProject(ctx); err != nil {
		return err
	}

	if opts.DoNotRecreate {
		return errors.Join(blob.ErrUnsupportedPutBlobOption, errDoNotRecreate)
	}

	// Initiate the upload of our Object to the specified bucket and key.
	// NOTE: UploadOptions currently only has ExpirationTime which defaults to 0 which is infinite
	upload, err := s.project.UploadObject(ctx, s.BucketName, string(b), &uplink.UploadOptions{})
	if err != nil {
		return fmt.Errorf("could not initiate upload: %w", err)
	}

	objMeta := newCustomMeta()
	if !opts.SetModTime.IsZero() {
		objMeta.SetLastModified(opts.SetModTime)
	}

	if opts.GetModTime != nil {
		*opts.GetModTime = objMeta.GetLastModified()
	}

	if err = upload.SetCustomMetadata(ctx, uplink.CustomMetadata(objMeta)); err != nil {
		_ = upload.Abort()
		return fmt.Errorf("could not set metadata: %w", err)
	}

	// Copy the data to the upload.
	_, err = io.Copy(upload, data.Reader())
	if err != nil {
		_ = upload.Abort()
		return fmt.Errorf("could not upload data: %w", err)
	}

	// Commit the uploaded object.
	err = upload.Commit()
	if err != nil {
		return fmt.Errorf("could not commit uploaded object: %w", err)
	}

	return convertKnownError(data.Reader().Close())
}

// DeleteBlob removes the blob from storage. Future Get() operations will fail with ErrNotFound.
func (s *StorjStorage) DeleteBlob(ctx context.Context, b blob.ID) (err error) {
	if _, err := s.GetProject(ctx); err != nil {
		return convertKnownError(err)
	}

	_, err = s.project.DeleteObject(ctx, s.BucketName, string(b))

	return convertKnownError(err)
}

// ExtendBlobRetention implements blob.Storage.
func (s *StorjStorage) ExtendBlobRetention(_ context.Context, _ blob.ID, _ blob.ExtendOptions) error {
	return fmt.Errorf("ExtendBlobRetention: %w", errNotImplemented)
}

// FlushCaches flushes any local caches associated with storage.
func (s *StorjStorage) FlushCaches(_ context.Context) error {
	return nil
}

// ConnectionInfo implements blob.Storage.
func (s *StorjStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   storjStorageType,
		Config: &s.Options,
	}
}

// DisplayName implements blob.Storage.
func (s *StorjStorage) DisplayName() string {
	return fmt.Sprintf("%s%s", storjSchemePfx, s.BucketName)
}

// GetBlob implements blob.Storage.
func (s *StorjStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The reason for the removed condition is not quite clear?!
	if length < -1 || offset < 0 /* (length > 0 && offset >= (offset+length-1)) */ {
		return fmt.Errorf("%w: offset(%d) length(%d)", blob.ErrInvalidRange, offset, length)
	}

	download, err := s.project.DownloadObject(ctx, s.BucketName, string(b), &uplink.DownloadOptions{Offset: offset, Length: length})
	if err != nil {
		return convertKnownError(err)
	}

	defer func() {
		if closeErr := download.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	br, err := io.Copy(output, download)

	if length != -1 && br != length {
		return fmt.Errorf("%w: %d (written to output) != %d (length)", ErrIncompleteIO, br, length)
	}

	return nil
}

// GetCapacity implements blob.Storage.
// FIXME: this doesn't seem right
// we should somehow query the project limits and return a value.
func (s *StorjStorage) GetCapacity(_ context.Context) (blob.Capacity, error) {
	return blob.Capacity{}, blob.ErrNotAVolume
}

// GetMetadata implements blob.Storage.
func (s *StorjStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	if _, err := s.GetProject(ctx); err != nil {
		return blob.Metadata{}, err
	}

	object, err := s.project.StatObject(ctx, s.BucketName, string(b))
	if err != nil {
		return blob.Metadata{}, convertKnownError(err)
	}

	tlm := customMeta(object.Custom).GetLastModifiedOrDefault(object.System.Created) // return last modified or if time(0) Created TS

	return blob.Metadata{
		BlobID:    b,
		Length:    object.System.ContentLength,
		Timestamp: tlm,
	}, nil
}

// IsReadOnly implements blob.Storage.
// TODO: check: why not implemented in e.g. s3, but leads to nil pointer if not implemented?!
func (s *StorjStorage) IsReadOnly() bool {
	return false
}

// ListBlobs implements blob.Storage, listing all blobs within the bucket matching the given prefix.
// Note: in kopia prefix is not the bucket but an arbitrary-length prefix of the blob ID.
func (s *StorjStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) (errs error) {
	var (
		err      error
		blobMeta blob.Metadata
	)

	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	project, err := s.GetProject(ctx)
	if err != nil {
		return err
	}

	objsIter := project.ListObjects(
		ctx, s.BucketName, &uplink.ListObjectsOptions{
			Prefix:    "",   // is prefix something like if blobID="abcdefghijklmn" then <pfx/id> is just the "namespace limiter" meaning objects <pfx>* | select(id)
			Recursive: true, // TODO: TBC  how are recursive prefixes handled? Probably /ab/cd/ef/abcdefghijklmn ?
			System:    true,
			Custom:    true,
		})

	for objsIter.Next() {
		// For this limited metadata, we don't need s.GetMetadata (this is a new RPC request!), we already have it in the items ListObjects returned
		// TODO:(?) probably more logical to extend uplink.Object with appropriate getters & setters
		objTS := customMeta(objsIter.Item().Custom).GetLastModifiedOrDefault(objsIter.Item().System.Created)

		blobMeta = blob.Metadata{
			BlobID:    blob.ID(objsIter.Item().Key),
			Length:    objsIter.Item().System.ContentLength,
			Timestamp: objTS,
		}
		if strings.HasPrefix(string(blobMeta.BlobID), string(prefix)) {
			err = callback(blobMeta)
		}

		errs = errors.Join(errs, err)
	}

	return errs
}

// basic bucket management also on the storage level for convenience (is logical as well?)
// TODO? we'd have to extend the blob.Storage interface for that... So we cannot access this for now.
// Come to think of it, since it's not kopia's responsibility to manage buckets, it will probably remain like this...
// Or: another take: in fact a bucket is the equivalent of a storage medium -> bucket is one level higher and thus "invisible" for kopia (storj-account > project > (bucket <-> kopia-storage) > kopia-repo)
// This also means that from storj POV `storjExternal` instance should hold login variables (singleton),
// but we should be able to instantiate it on the kopia-storage level

func init() {
	blob.AddSupportedStorage(storjStorageType, Options{}, New)
}
