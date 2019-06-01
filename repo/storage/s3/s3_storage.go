// Package s3 implements Storage based on an S3 bucket.
package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/efarrer/iothrottler"
	"github.com/minio/minio-go"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/storage"
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

func (s *s3Storage) GetBlock(ctx context.Context, b string, offset, length int64) ([]byte, error) {
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

	v, err := exponentialBackoff(fmt.Sprintf("GetBlock(%q,%v,%v)", b, offset, length), attempt)
	if err != nil {
		return nil, translateError(err)
	}

	return v.([]byte), nil
}

func exponentialBackoff(desc string, att retry.AttemptFunc) (interface{}, error) {
	return retry.WithExponentialBackoff(desc, att, isRetriableError)
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
		if me.StatusCode == 200 {
			return nil
		}
		if me.StatusCode == 404 {
			return storage.ErrBlockNotFound
		}
	}

	return err
}

func (s *s3Storage) PutBlock(ctx context.Context, b string, data []byte) error {
	throttled, err := s.uploadThrottler.AddReader(ioutil.NopCloser(bytes.NewReader(data)))
	if err != nil {
		return err
	}

	progressCallback := storage.ProgressCallback(ctx)
	if progressCallback != nil {
		progressCallback(b, 0, int64(len(data)))
		defer progressCallback(b, int64(len(data)), int64(len(data)))
	}
	n, err := s.cli.PutObject(s.BucketName, s.getObjectNameString(b), throttled, -1, minio.PutObjectOptions{
		ContentType: "application/x-kopia",
		Progress:    newProgressReader(progressCallback, b, int64(len(data))),
	})
	if err == io.EOF && n == 0 {
		// special case empty stream
		_, err = s.cli.PutObject(s.BucketName, s.getObjectNameString(b), bytes.NewBuffer(nil), 0, minio.PutObjectOptions{
			ContentType: "application/x-kopia",
		})
	}

	return translateError(err)
}

func (s *s3Storage) DeleteBlock(ctx context.Context, b string) error {
	attempt := func() (interface{}, error) {
		return nil, s.cli.RemoveObject(s.BucketName, s.getObjectNameString(b))
	}

	_, err := exponentialBackoff(fmt.Sprintf("DeleteBlock(%q)", b), attempt)
	return translateError(err)
}

func (s *s3Storage) getObjectNameString(b string) string {
	return s.Prefix + b
}

func (s *s3Storage) ListBlocks(ctx context.Context, prefix string, callback func(storage.BlockMetadata) error) error {
	oi := s.cli.ListObjects(s.BucketName, s.Prefix+prefix, false, ctx.Done())
	for o := range oi {
		if err := o.Err; err != nil {
			return err
		}

		bm := storage.BlockMetadata{
			BlockID:   o.Key[len(s.Prefix):],
			Length:    o.Size,
			Timestamp: o.LastModified,
		}

		if err := callback(bm); err != nil {
			return err
		}
	}

	return nil
}

func (s *s3Storage) ConnectionInfo() storage.ConnectionInfo {
	return storage.ConnectionInfo{
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
	cb           storage.ProgressFunc
	blockID      string
	completed    int64
	totalLength  int64
	lastReported int64
}

func (r *progressReader) Read(b []byte) (int, error) {
	r.completed += int64(len(b))
	if r.completed >= r.lastReported+1000000 && r.completed < r.totalLength {
		r.cb(r.blockID, r.completed, r.totalLength)
		r.lastReported = r.completed
	}
	return len(b), nil
}

func newProgressReader(cb storage.ProgressFunc, blockID string, totalLength int64) io.Reader {
	if cb == nil {
		return nil
	}

	return &progressReader{cb: cb, blockID: blockID, totalLength: totalLength}
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
func New(ctx context.Context, opt *Options) (storage.Storage, error) {
	if opt.BucketName == "" {
		return nil, errors.New("bucket name must be specified")
	}

	cli, err := minio.New(opt.Endpoint, opt.AccessKeyID, opt.SecretAccessKey, !opt.DoNotUseTLS)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create client")
	}

	downloadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxDownloadSpeedBytesPerSecond))
	uploadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxUploadSpeedBytesPerSecond))

	return &s3Storage{
		Options:           *opt,
		ctx:               ctx,
		cli:               cli,
		downloadThrottler: downloadThrottler,
		uploadThrottler:   uploadThrottler,
	}, nil
}

func init() {
	storage.AddSupportedStorage(
		s3storageType,
		func() interface{} {
			return &Options{}
		},
		func(ctx context.Context, o interface{}) (storage.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
