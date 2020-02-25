// Package s3 implements Storage based on an S3 bucket.
package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/efarrer/iothrottler"
	minio "github.com/minio/minio-go/v6"
	"github.com/minio/minio-go/v6/pkg/credentials"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
)

const (
	s3storageType = "s3"
)

type s3Storage struct {
	Options

	ctx context.Context

	cli *minio.Client

	downloadThrottler *iothrottler.IOThrottlerPool
	uploadThrottler   *iothrottler.IOThrottlerPool
}

func (s *s3Storage) GetBlob(ctx context.Context, b blob.ID, offset, length int64) ([]byte, error) {
	attempt := func() (interface{}, error) {
		var opt minio.GetObjectOptions

		if length > 0 {
			if err := opt.SetRange(offset, offset+length-1); err != nil {
				return nil, errors.Wrap(err, "unable to set range")
			}
		}

		o, err := s.cli.GetObject(s.BucketName, s.getObjectNameString(b), opt)
		if err != nil {
			return 0, err
		}

		defer o.Close() //nolint:errcheck

		throttled, err := s.downloadThrottler.AddReader(o)
		if err != nil {
			return nil, err
		}

		b, err := ioutil.ReadAll(throttled)
		if err != nil {
			return nil, err
		}

		if len(b) != int(length) && length > 0 {
			return nil, errors.Errorf("invalid length, got %v bytes, but expected %v", len(b), length)
		}

		if length == 0 {
			return []byte{}, nil
		}

		return b, nil
	}

	v, err := exponentialBackoff(ctx, fmt.Sprintf("GetBlob(%q,%v,%v)", b, offset, length), attempt)
	if err != nil {
		return nil, translateError(err)
	}

	return v.([]byte), nil
}

func exponentialBackoff(ctx context.Context, desc string, att retry.AttemptFunc) (interface{}, error) {
	return retry.WithExponentialBackoff(ctx, desc, att, isRetriableError)
}

func isRetriableError(err error) bool {
	if me, ok := err.(minio.ErrorResponse); ok {
		// retry on server errors, not on client errors
		return me.StatusCode >= 500
	}

	return false
}

func translateError(err error) error {
	if me, ok := err.(minio.ErrorResponse); ok {
		if me.StatusCode == http.StatusOK {
			return nil
		}

		if me.StatusCode == http.StatusNotFound {
			return blob.ErrBlobNotFound
		}
	}

	return err
}

func (s *s3Storage) PutBlob(ctx context.Context, b blob.ID, data []byte) error {
	throttled, err := s.uploadThrottler.AddReader(ioutil.NopCloser(bytes.NewReader(data)))
	if err != nil {
		return err
	}

	progressCallback := blob.ProgressCallback(ctx)
	if progressCallback != nil {
		progressCallback(string(b), 0, int64(len(data)))
		defer progressCallback(string(b), int64(len(data)), int64(len(data)))
	}

	n, err := s.cli.PutObject(s.BucketName, s.getObjectNameString(b), throttled, -1, minio.PutObjectOptions{
		ContentType: "application/x-kopia",
		Progress:    newProgressReader(progressCallback, string(b), int64(len(data))),
	})

	if err == io.EOF && n == 0 {
		// special case empty stream
		_, err = s.cli.PutObject(s.BucketName, s.getObjectNameString(b), bytes.NewBuffer(nil), 0, minio.PutObjectOptions{
			ContentType: "application/x-kopia",
		})
	}

	return translateError(err)
}

func (s *s3Storage) DeleteBlob(ctx context.Context, b blob.ID) error {
	attempt := func() (interface{}, error) {
		return nil, s.cli.RemoveObject(s.BucketName, s.getObjectNameString(b))
	}

	_, err := exponentialBackoff(ctx, fmt.Sprintf("DeleteBlob(%q)", b), attempt)

	return translateError(err)
}

func (s *s3Storage) getObjectNameString(b blob.ID) string {
	return s.Prefix + string(b)
}

func (s *s3Storage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	oi := s.cli.ListObjects(s.BucketName, s.getObjectNameString(prefix), false, ctx.Done())
	for o := range oi {
		if err := o.Err; err != nil {
			return err
		}

		bm := blob.Metadata{
			BlobID:    blob.ID(o.Key[len(s.Prefix):]),
			Length:    o.Size,
			Timestamp: o.LastModified,
		}

		if err := callback(bm); err != nil {
			return err
		}
	}

	return nil
}

func (s *s3Storage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   s3storageType,
		Config: &s.Options,
	}
}

func (s *s3Storage) Close(ctx context.Context) error {
	return nil
}

func (s *s3Storage) String() string {
	return fmt.Sprintf("s3://%v/%v", s.BucketName, s.Prefix)
}

type progressReader struct {
	cb           blob.ProgressFunc
	blobID       string
	completed    int64
	totalLength  int64
	lastReported int64
}

func (r *progressReader) Read(b []byte) (int, error) {
	r.completed += int64(len(b))
	if r.completed >= r.lastReported+1000000 && r.completed < r.totalLength {
		r.cb(r.blobID, r.completed, r.totalLength)
		r.lastReported = r.completed
	}

	return len(b), nil
}

func newProgressReader(cb blob.ProgressFunc, blobID string, totalLength int64) io.Reader {
	if cb == nil {
		return nil
	}

	return &progressReader{cb: cb, blobID: blobID, totalLength: totalLength}
}

func toBandwidth(bytesPerSecond int) iothrottler.Bandwidth {
	if bytesPerSecond <= 0 {
		return iothrottler.Unlimited
	}

	return iothrottler.Bandwidth(bytesPerSecond) * iothrottler.BytesPerSecond
}

// New creates new S3-backed storage with specified options:
//
// - the 'BucketName' field is required and all other parameters are optional.
func New(ctx context.Context, opt *Options) (blob.Storage, error) {
	if opt.BucketName == "" {
		return nil, errors.New("bucket name must be specified")
	}

	cli, err := minio.NewWithCredentials(opt.Endpoint, credentials.NewStaticV4(opt.AccessKeyID, opt.SecretAccessKey, opt.SessionToken), !opt.DoNotUseTLS, opt.Region)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create client")
	}

	downloadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxDownloadSpeedBytesPerSecond))
	uploadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxUploadSpeedBytesPerSecond))

	ok, err := cli.BucketExistsWithContext(ctx, opt.BucketName)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to determine if bucket %q exists", opt.BucketName)
	}

	if !ok {
		return nil, errors.Errorf("bucket %q does not exist", opt.BucketName)
	}

	return &s3Storage{
		Options:           *opt,
		ctx:               ctx,
		cli:               cli,
		downloadThrottler: downloadThrottler,
		uploadThrottler:   uploadThrottler,
	}, nil
}

func init() {
	blob.AddSupportedStorage(
		s3storageType,
		func() interface{} {
			return &Options{}
		},
		func(ctx context.Context, o interface{}) (blob.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
