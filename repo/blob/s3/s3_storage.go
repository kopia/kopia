// Package s3 implements Storage based on an S3 bucket.
package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/efarrer/iothrottler"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	s3storageType = "s3"
)

type s3Storage struct {
	sendMD5 int32
	Options

	cli *minio.Client

	downloadThrottler *iothrottler.IOThrottlerPool
	uploadThrottler   *iothrottler.IOThrottlerPool
}

func (s *s3Storage) GetBlob(ctx context.Context, b blob.ID, offset, length int64) ([]byte, error) {
	attempt := func() ([]byte, error) {
		var opt minio.GetObjectOptions

		if length > 0 {
			if err := opt.SetRange(offset, offset+length-1); err != nil {
				return nil, errors.Wrap(blob.ErrInvalidRange, "unable to set range")
			}
		}

		o, err := s.cli.GetObject(ctx, s.BucketName, s.getObjectNameString(b), opt)
		if err != nil {
			return nil, errors.Wrap(err, "GetObject")
		}

		defer o.Close() //nolint:errcheck

		throttled, err := s.downloadThrottler.AddReader(o)
		if err != nil {
			return nil, errors.Wrap(err, "AddReader")
		}

		v, err := ioutil.ReadAll(throttled)
		if err != nil {
			return nil, errors.Wrap(err, "ReadAll")
		}

		if length == 0 {
			return []byte{}, nil
		}

		return v, nil
	}

	fetched, err := attempt()
	if err != nil {
		return nil, translateError(err)
	}

	return blob.EnsureLengthExactly(fetched, length)
}

func translateError(err error) error {
	var me minio.ErrorResponse

	if errors.As(err, &me) {
		switch me.StatusCode {
		case http.StatusOK:
			return nil

		case http.StatusNotFound:
			return blob.ErrBlobNotFound

		case http.StatusRequestedRangeNotSatisfiable:
			return blob.ErrInvalidRange
		}
	}

	return err
}

func (s *s3Storage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	oi, err := s.cli.StatObject(ctx, s.BucketName, s.getObjectNameString(b), minio.StatObjectOptions{})
	if err != nil {
		return blob.Metadata{}, errors.Wrap(translateError(err), "StatObject")
	}

	return blob.Metadata{
		BlobID:    b,
		Length:    oi.Size,
		Timestamp: oi.LastModified,
	}, nil
}

func (s *s3Storage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes) error {
	throttled, err := s.uploadThrottler.AddReader(ioutil.NopCloser(data.Reader()))
	if err != nil {
		return errors.Wrap(err, "AddReader")
	}

	uploadInfo, err := s.cli.PutObject(ctx, s.BucketName, s.getObjectNameString(b), throttled, int64(data.Length()), minio.PutObjectOptions{
		ContentType:    "application/x-kopia",
		SendContentMd5: atomic.LoadInt32(&s.sendMD5) > 0,
	})

	var er minio.ErrorResponse

	if errors.As(err, &er) && er.Code == "InvalidRequest" && strings.Contains(strings.ToLower(er.Message), "content-md5") {
		atomic.StoreInt32(&s.sendMD5, 1) // set sendMD5 on retry

		return err // nolint:wrapcheck
	}

	if errors.Is(err, io.EOF) && uploadInfo.Size == 0 {
		// special case empty stream
		_, err = s.cli.PutObject(ctx, s.BucketName, s.getObjectNameString(b), bytes.NewBuffer(nil), 0, minio.PutObjectOptions{
			ContentType: "application/x-kopia",
		})
	}

	// nolint:wrapcheck
	return err
}

func (s *s3Storage) SetTime(ctx context.Context, b blob.ID, t time.Time) error {
	return blob.ErrSetTimeUnsupported
}

func (s *s3Storage) DeleteBlob(ctx context.Context, b blob.ID) error {
	err := translateError(s.cli.RemoveObject(ctx, s.BucketName, s.getObjectNameString(b), minio.RemoveObjectOptions{}))
	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}

	return err
}

func (s *s3Storage) getObjectNameString(b blob.ID) string {
	return s.Prefix + string(b)
}

func (s *s3Storage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	oi := s.cli.ListObjects(ctx, s.BucketName, minio.ListObjectsOptions{
		Prefix: s.getObjectNameString(prefix),
	})
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

func (s *s3Storage) DisplayName() string {
	return fmt.Sprintf("S3: %v %v", s.Endpoint, s.BucketName)
}

func toBandwidth(bytesPerSecond int) iothrottler.Bandwidth {
	if bytesPerSecond <= 0 {
		return iothrottler.Unlimited
	}

	return iothrottler.Bandwidth(bytesPerSecond) * iothrottler.BytesPerSecond
}

func getCustomTransport(insecureSkipVerify bool) (transport *http.Transport) {
	// nolint:gosec
	customTransport := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: insecureSkipVerify}}
	return customTransport
}

// New creates new S3-backed storage with specified options:
//
// - the 'BucketName' field is required and all other parameters are optional.
func New(ctx context.Context, opt *Options) (blob.Storage, error) {
	if opt.BucketName == "" {
		return nil, errors.New("bucket name must be specified")
	}

	minioOpts := &minio.Options{
		Creds:  credentials.NewStaticV4(opt.AccessKeyID, opt.SecretAccessKey, opt.SessionToken),
		Secure: !opt.DoNotUseTLS,
		Region: opt.Region,
	}

	if opt.DoNotVerifyTLS {
		minioOpts.Transport = getCustomTransport(true)
	}

	cli, err := minio.New(opt.Endpoint, minioOpts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create client")
	}

	downloadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxDownloadSpeedBytesPerSecond))
	uploadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxUploadSpeedBytesPerSecond))

	ok, err := cli.BucketExists(ctx, opt.BucketName)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to determine if bucket %q exists", opt.BucketName)
	}

	if !ok {
		return nil, errors.Errorf("bucket %q does not exist", opt.BucketName)
	}

	return retrying.NewWrapper(&s3Storage{
		Options:           *opt,
		cli:               cli,
		sendMD5:           0,
		downloadThrottler: downloadThrottler,
		uploadThrottler:   uploadThrottler,
	}), nil
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
