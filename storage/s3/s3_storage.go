package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/efarrer/iothrottler"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/storage"
	"github.com/minio/minio-go"
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

func (s *s3Storage) GetBlock(b string, offset, length int64) ([]byte, error) {
	attempt := func() (interface{}, error) {
		var opt minio.GetObjectOptions
		if length > 0 {
			opt.SetRange(offset, offset+length)
		}

		o, err := s.cli.GetObject(s.BucketName, s.getObjectNameString(b), opt)
		if err != nil {
			return 0, err
		}

		defer o.Close()
		throttled, err := s.downloadThrottler.AddReader(o)
		if err != nil {
			return nil, err
		}

		return ioutil.ReadAll(throttled)
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

	switch err {
	case nil:
		return false
	default:
		return true
	}
}

func translateError(err error) error {
	if me, ok := err.(minio.ErrorResponse); ok {
		if me.StatusCode == 404 {
			return storage.ErrBlockNotFound
		}
	}

	switch err {
	case nil:
		return nil
	default:
		return fmt.Errorf("unexpected S3 error: %v", err)
	}
}

func (s *s3Storage) PutBlock(b string, data []byte) error {
	attempt := func() (interface{}, error) {
		rc := ioutil.NopCloser(bytes.NewReader(data))
		throttled, err := s.uploadThrottler.AddReader(rc)
		if err != nil {
			return nil, err
		}

		n, err := s.cli.PutObject(s.BucketName, s.getObjectNameString(b), throttled, int64(len(data)), minio.PutObjectOptions{})
		if err != nil {
			return nil, err
		}

		if n != int64(len(data)) {
			return nil, fmt.Errorf("truncated write %v of %v bytes", n, len(data))
		}

		return nil, nil
	}

	_, err := exponentialBackoff(fmt.Sprintf("PutBlock(%q)", b), attempt)
	return translateError(err)
}

func (s *s3Storage) DeleteBlock(b string) error {
	attempt := func() (interface{}, error) {
		return nil, s.cli.RemoveObject(s.BucketName, s.getObjectNameString(b))
	}

	_, err := exponentialBackoff(fmt.Sprintf("DeleteBlock(%q)", b), attempt)
	return translateError(err)
}

func (s *s3Storage) getObjectNameString(b string) string {
	return s.Prefix + b
}

func (s *s3Storage) ListBlocks(prefix string) (<-chan storage.BlockMetadata, storage.CancelFunc) {
	ch := make(chan storage.BlockMetadata, 100)
	cancelled := make(chan struct{})

	go func() {
		defer close(ch)

		oi := s.cli.ListObjects(s.BucketName, s.Prefix+prefix, false, cancelled)
		for o := range oi {
			if err := o.Err; err != nil {
				select {
				case ch <- storage.BlockMetadata{Error: translateError(err)}:
					return
				case <-cancelled:
					return
				}
			}

			bm := storage.BlockMetadata{
				BlockID:   o.Key[len(s.Prefix):],
				Length:    o.Size,
				TimeStamp: o.LastModified,
			}

			select {
			case ch <- bm:
			case <-cancelled:
				return
			}
		}
	}()

	return ch, func() {
		close(cancelled)
	}
}

func (s *s3Storage) ConnectionInfo() storage.ConnectionInfo {
	return storage.ConnectionInfo{
		Type:   s3storageType,
		Config: &s.Options,
	}
}

func (s *s3Storage) Close() error {
	return nil
}

func (s *s3Storage) String() string {
	return fmt.Sprintf("s3://%v/%v", s.BucketName, s.Prefix)
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
		return nil, fmt.Errorf("unable to create client: %v", err)
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
