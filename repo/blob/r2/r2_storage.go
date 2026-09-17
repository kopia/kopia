// Package r2 implements Storage based on a Cloudflare R2 bucket.
package r2

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/s3"
)

type r2Storage struct {
	blob.Storage

	opt Options
}

func (s *r2Storage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	if opts.HasRetentionOptions() {
		// R2 does not support S3 Object Lock headers, so fail before the S3
		// compatibility layer can send retention options to Cloudflare.
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "blob-retention")
	}

	return s.Storage.PutBlob(ctx, b, data, opts) //nolint:wrapcheck
}

func (s *r2Storage) ExtendBlobRetention(ctx context.Context, b blob.ID, opts blob.ExtendOptions) error {
	return blob.ErrUnsupportedObjectLock
}

func (s *r2Storage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   StorageType,
		Config: &s.opt,
	}
}

func (s *r2Storage) DisplayName() string {
	endpoint, _, err := s.opt.s3Endpoint()
	if err != nil {
		endpoint = s.opt.Endpoint
	}

	return fmt.Sprintf("Cloudflare R2: %v %v", endpoint, s.opt.BucketName)
}

func (s *r2Storage) String() string {
	return fmt.Sprintf("r2://%v/%v", s.opt.BucketName, s.opt.Prefix)
}

// New creates new Cloudflare R2-backed storage with specified options.
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
	s3Options, err := opt.toS3Options()
	if err != nil {
		return nil, err
	}

	st, err := s3.New(ctx, s3Options, isCreate)
	if err != nil {
		return nil, err
	}

	return &r2Storage{
		Storage: st,
		opt:     *opt,
	}, nil
}

func init() {
	blob.AddSupportedStorage(StorageType, Options{}, New)
}
