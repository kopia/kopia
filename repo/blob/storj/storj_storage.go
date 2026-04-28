package storj

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"storj.io/uplink"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	storjStorageType = "storj"
	storjSchemePfx   = "sj://"
)

var ErrIncompleteIO = errors.New("bytes read/written != buffer length")

// TODO: remove if unused
// type storjPointInTimeStorage struct {
// 	StorjStorage
// 	pointInTime time.Time
// }

type StorjStorage struct {
	blob.Storage // why does s3 not (need to) encapsulate this???
	Options
	// storjExt      *storjExternal // more logical to hold this instance directly here, and "connection" abstractions as singleton instances under it
	project       *uplink.Project
	encrypted     bool
	storageConfig *StorageConfig
}

// enriched uplink.CustomMetadata for convenient conversions  to known field types
type customMeta uplink.CustomMetadata

func newCustomMeta() (cm customMeta) {
	cm = make(customMeta)
	cm["ContentType"] = "application/x-kopia"
	cm.SetLastModified(time.Now()) // assumes instantiation of customMeta is "close enough" to object creation
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

func New(ctx context.Context, opt *Options, createBucket bool) (blob.Storage, error) {
	st, err := newStorage(ctx, opt, createBucket)
	if err != nil {
		return nil, err
	}

	if st == nil {
		return nil, fmt.Errorf("storj_storage::New: st nil while err != nil: this should never happen!")
	}
	// s, err := maybePointInTimeStore(ctx, st, opt.PointInTime)
	// if err != nil {
	// 	return nil, err
	// }

	return retrying.NewWrapper(st), nil
}

func NewUnwrapped(ctx context.Context, opt *Options, createBucket bool) (blob.Storage, error) {
	st, err := newStorage(ctx, opt, createBucket)
	if err != nil {
		return nil, err
	}

	if st == nil {
		return nil, fmt.Errorf("storj_storage::New: st nil while err != nil: this should never happen!")
	}
	return st, nil
}

func (s *StorjStorage) Close(ctx context.Context) (err error) {
	if s.project == nil {
		return errors.New("refusing to close nil project")
	}
	return s.project.Close()
}

func newStorage(ctx context.Context, opt *Options, isCreate bool) (storjStorage *StorjStorage, err error) {
	// stext := NewStorjExternal()

	// Not so clean, because mutates some fields related to access in opt,
	// but at the same time we use opt to pass any such info via opt.
	// So basically it is some funky optional behaviour?!
	/*
		err = SetupAccess(ctx, opt)
		if err != nil {
			return nil, err
		}
	*/

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
		_, err = proj.EnsureBucket(ctx, opt.BucketName)
	} else {
		_, err = proj.StatBucket(ctx, opt.BucketName)
	}
	if err != nil {
		return nil, err
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

func (s *StorjStorage) GetProject(ctx context.Context) (project *uplink.Project, err error) {
	if s.project != nil {
		return s.project, nil
	}
	// TODO: remove the API key access method from options/docs
	access, err := uplink.ParseAccess(s.Options.KeyOrGrant)
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
	// return s.storjExt.GetProject(ctx, s.Options.KeyOrGrant)
}

// ############################ blob interface implementation
func (s *StorjStorage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	if s.BucketName == "" {
		return errors.Join(blob.ErrNotAVolume, errors.New("missing bucket name"))
	}
	if _, err := s.GetProject(ctx); err != nil {
		return err
	}
	if opts.DoNotRecreate {
		return errors.Join(blob.ErrUnsupportedPutBlobOption, errors.New("do-not-recreate"))
	}

	// Intitiate the upload of our Object to the specified bucket and key.
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

	upload.SetCustomMetadata(ctx, uplink.CustomMetadata(objMeta))

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

func (s *StorjStorage) ExtendBlobRetention(ctx context.Context, b blob.ID, opts blob.ExtendOptions) error {
	return fmt.Errorf("StorjStorage::ExtendBlobRetention() not yet implemented!")
}

// FlushCaches flushes any local caches associated with storage.
func (s *StorjStorage) FlushCaches(ctx context.Context) error {
	return nil
}

func (s *StorjStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   storjStorageType,
		Config: &s.Options,
	}
}

func (s *StorjStorage) DisplayName() string {
	return fmt.Sprintf("%s%s", storjSchemePfx, s.BucketName)
}

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

// FIXME: this doesn't seem right
// we should somehow query the project limits and return a value
func (s *StorjStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	return blob.Capacity{}, blob.ErrNotAVolume
}

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

// TODO: check: why not implemented in e.g. s3, but leads to nil pointer if not implemented?!
func (s *StorjStorage) IsReadOnly() bool {
	return false
}

// In kopia prefix is *not* the bucket, but just an (arbitrary length?) start string of the blob ID
// In uplink, prefix seems to be a "directory"(ish) (TBC)
// The expectation of the Storage interface seems to be that ListBlobs only lists all blobs *within the opened bucket*
// (this is the reason to have a fixed bucket within a storjStorage instance)
func (s *StorjStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) (errs error) {
	var err error
	var blobMeta blob.Metadata

	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	project, err := s.GetProject(ctx)
	if err != nil {
		return err
	}

	// if prefix != "" && !strings.HasSuffix(string(prefix), "/") {
	// 	prefix = prefix + "/"
	// }

	objsIter := project.ListObjects(
		ctx, s.BucketName, &uplink.ListObjectsOptions{
			Prefix:    "",   // is prefix something like if blobID="abcdefghijklmn" then <pfx/id> is just the "namespace limiter" meaning objects <pfx>* | select(id)
			Recursive: true, // TODO: TBC  how are recursive prefixes handled? Probably /ab/cd/ef/abcdefghijklmn ?
			System:    true,
			Custom:    true,
		})

	for objsIter.Next() {
		// For this limited metadata, we don't need s.GetMetadata (this is a new RPC request!), we already have it in the items ListObjects returned
		// blobMeta, err = s.GetMetadata(ctx, blob.ID(objsIter.Item().Key))
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
