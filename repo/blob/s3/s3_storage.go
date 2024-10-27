// Package s3 implements Storage based on an S3 bucket.
package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	s3storageType   = "s3"
	latestVersionID = ""
)

type s3Storage struct {
	Options
	blob.DefaultProviderImplementation

	cli *minio.Client

	storageConfig *StorageConfig
}

func (s *s3Storage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	return s.getBlobWithVersion(ctx, b, latestVersionID, offset, length, output)
}

// getBlobWithVersion returns full or partial contents of a blob with given ID and version.
func (s *s3Storage) getBlobWithVersion(ctx context.Context, b blob.ID, version string, offset, length int64, output blob.OutputBuffer) error {
	output.Reset()

	attempt := func() error {
		opt := minio.GetObjectOptions{VersionID: version}

		if length > 0 {
			if err := opt.SetRange(offset, offset+length-1); err != nil {
				return errors.Wrap(blob.ErrInvalidRange, "unable to set range")
			}
		}

		if length == 0 {
			// zero-length ranges require special handling, set non-zero range and
			// we won't be trying to read the response anyway.
			if err := opt.SetRange(0, 1); err != nil {
				return errors.Wrap(blob.ErrInvalidRange, "unable to set range")
			}
		}

		o, err := s.cli.GetObject(ctx, s.BucketName, s.getObjectNameString(b), opt)
		if err != nil {
			return errors.Wrap(err, "GetObject")
		}

		defer o.Close() //nolint:errcheck

		if length == 0 {
			return nil
		}

		return iocopy.JustCopy(output, o)
	}

	if err := attempt(); err != nil {
		return translateError(err)
	}

	//nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func isInvalidCredentials(err error) bool {
	return err != nil && strings.Contains(err.Error(), blob.InvalidCredentialsErrStr)
}

func translateError(err error) error {
	var me minio.ErrorResponse

	if isInvalidCredentials(err) {
		return blob.ErrInvalidCredentials
	}

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
	vm, err := s.getVersionMetadata(ctx, b, "")

	return vm.Metadata, err
}

func (s *s3Storage) getVersionMetadata(ctx context.Context, b blob.ID, version string) (versionMetadata, error) {
	opts := minio.GetObjectOptions{
		VersionID: version,
	}

	oi, err := s.cli.StatObject(ctx, s.BucketName, s.getObjectNameString(b), opts)
	if err != nil {
		return versionMetadata{}, errors.Wrap(translateError(err), "StatObject")
	}

	return infoToVersionMetadata(s.Prefix, &oi), nil
}

func (s *s3Storage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	switch {
	case opts.DoNotRecreate:
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "do-not-recreate")
	case !opts.SetModTime.IsZero():
		return blob.ErrSetTimeUnsupported
	}

	_, err := s.putBlob(ctx, b, data, opts)

	if opts.GetModTime != nil {
		bm, err2 := s.GetMetadata(ctx, b)
		if err2 != nil {
			return err2
		}

		*opts.GetModTime = bm.Timestamp
	}

	return err
}

func (s *s3Storage) putBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) (versionMetadata, error) {
	var (
		storageClass    = s.storageConfig.getStorageClassForBlobID(b)
		retentionMode   minio.RetentionMode
		retainUntilDate time.Time
	)

	if opts.RetentionPeriod != 0 {
		retentionMode = minio.RetentionMode(opts.RetentionMode)
		if !retentionMode.IsValid() {
			return versionMetadata{}, errors.Errorf("invalid retention mode: %q", opts.RetentionMode)
		}

		retainUntilDate = clock.Now().Add(opts.RetentionPeriod).UTC()
	}

	uploadInfo, err := s.cli.PutObject(ctx, s.BucketName, s.getObjectNameString(b), data.Reader(), int64(data.Length()), minio.PutObjectOptions{
		ContentType: "application/x-kopia",
		// Kopia already splits snapshot contents into small blobs to improve
		// upload throughput. There is no need for further splitting
		// through multipart uploads.
		DisableMultipart: true,
		// The Content-MD5 header is required for any request to upload an object
		// with a retention period configured using Amazon S3 Object Lock.
		// Unconditionally computing the content MD5, potentially incurring
		// a slightly higher CPU overhead.
		SendContentMd5:  true,
		StorageClass:    storageClass,
		RetainUntilDate: retainUntilDate,
		Mode:            retentionMode,
	})

	if isInvalidCredentials(err) {
		return versionMetadata{}, blob.ErrInvalidCredentials
	}

	var er minio.ErrorResponse

	if errors.As(err, &er) && er.Code == "InvalidRequest" && strings.Contains(strings.ToLower(er.Message), "content-md5") {
		return versionMetadata{}, err //nolint:wrapcheck
	}

	if errors.Is(err, io.EOF) && uploadInfo.Size == 0 {
		// special case empty stream
		_, err = s.cli.PutObject(ctx, s.BucketName, s.getObjectNameString(b), bytes.NewBuffer(nil), 0, minio.PutObjectOptions{
			ContentType:     "application/x-kopia",
			StorageClass:    storageClass,
			RetainUntilDate: retainUntilDate,
			Mode:            retentionMode,
		})
	}

	if err != nil {
		return versionMetadata{}, err //nolint:wrapcheck
	}

	return versionMetadata{
		Metadata: blob.Metadata{
			BlobID:    b,
			Length:    uploadInfo.Size,
			Timestamp: uploadInfo.LastModified,
		},
		Version: uploadInfo.VersionID,
	}, nil
}

func (s *s3Storage) DeleteBlob(ctx context.Context, b blob.ID) error {
	err := translateError(s.cli.RemoveObject(ctx, s.BucketName, s.getObjectNameString(b), minio.RemoveObjectOptions{}))
	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}

	return err
}

func (s *s3Storage) ExtendBlobRetention(ctx context.Context, b blob.ID, opts blob.ExtendOptions) error {
	retentionMode := minio.RetentionMode(opts.RetentionMode)
	if !retentionMode.IsValid() {
		return errors.Errorf("invalid retention mode: %q", opts.RetentionMode)
	}

	retainUntilDate := clock.Now().Add(opts.RetentionPeriod).UTC()

	err := s.cli.PutObjectRetention(ctx, s.BucketName, s.getObjectNameString(b), minio.PutObjectRetentionOptions{
		RetainUntilDate: &retainUntilDate,
		Mode:            &retentionMode,
	})
	if err != nil {
		return errors.Wrap(err, "unable to extend retention period")
	}

	return nil
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
			if isInvalidCredentials(err) {
				return blob.ErrInvalidCredentials
			}

			return err
		}

		bm := blob.Metadata{
			BlobID:    blob.ID(o.Key[len(s.Prefix):]),
			Length:    o.Size,
			Timestamp: o.LastModified,
		}

		if bm.BlobID == ConfigName {
			continue
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

func (s *s3Storage) String() string {
	return fmt.Sprintf("s3://%v/%v", s.BucketName, s.Prefix)
}

func (s *s3Storage) DisplayName() string {
	return fmt.Sprintf("S3: %v %v", s.Endpoint, s.BucketName)
}

func getCustomTransport(opt *Options) (*http.Transport, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert

	if opt.DoNotVerifyTLS {
		//nolint:gosec
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

		return transport, nil
	}

	if len(opt.RootCA) != 0 {
		rootcas := x509.NewCertPool()

		if ok := rootcas.AppendCertsFromPEM(opt.RootCA); !ok {
			return nil, errors.New("cannot parse provided CA")
		}

		transport.TLSClientConfig.RootCAs = rootcas
	}

	return transport, nil
}

// New creates new S3-backed storage with specified options:
//
// - the 'BucketName' field is required and all other parameters are optional.
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
	_ = isCreate

	st, err := newStorage(ctx, opt)
	if err != nil {
		return nil, err
	}

	s, err := maybePointInTimeStore(ctx, st, opt.PointInTime)
	if err != nil {
		return nil, err
	}

	return retrying.NewWrapper(s), nil
}

func newStorage(ctx context.Context, opt *Options) (*s3Storage, error) {
	creds := credentials.NewChainCredentials(
		[]credentials.Provider{
			&credentials.Static{
				Value: credentials.Value{
					AccessKeyID:     opt.AccessKeyID,
					SecretAccessKey: opt.SecretAccessKey,
					SessionToken:    opt.SessionToken,
					SignerType:      credentials.SignatureV4,
				},
			},
			&credentials.EnvAWS{},
			&credentials.IAM{
				Client: &http.Client{
					Transport: http.DefaultTransport,
				},
			},
		},
	)

	return newStorageWithCredentials(ctx, creds, opt)
}

func newStorageWithCredentials(ctx context.Context, creds *credentials.Credentials, opt *Options) (*s3Storage, error) {
	if opt.BucketName == "" {
		return nil, errors.New("bucket name must be specified")
	}

	minioOpts := &minio.Options{
		Creds:  creds,
		Secure: !opt.DoNotUseTLS,
		Region: opt.Region,
	}

	var err error

	minioOpts.Transport, err = getCustomTransport(opt)
	if err != nil {
		return nil, err
	}

	cli, err := minio.New(opt.Endpoint, minioOpts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create client")
	}

	s := s3Storage{
		Options:       *opt,
		cli:           cli,
		storageConfig: &StorageConfig{},
	}

	var scOutput gather.WriteBuffer

	if getBlobErr := s.GetBlob(ctx, ConfigName, 0, -1, &scOutput); getBlobErr == nil {
		if scErr := s.storageConfig.Load(scOutput.Bytes().Reader()); scErr != nil {
			return nil, errors.Wrapf(scErr, "error parsing storage config for bucket %q", opt.BucketName)
		}
	} else if !errors.Is(getBlobErr, blob.ErrBlobNotFound) {
		return nil, errors.Wrapf(getBlobErr, "error retrieving storage config from bucket %q", opt.BucketName)
	}

	return &s, nil
}

func init() {
	blob.AddSupportedStorage(s3storageType, Options{}, New)
}
