// Package b2 implements Storage based on an Backblaze B2 bucket.
package b2

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/efarrer/iothrottler"
	"github.com/pkg/errors"
	backblaze "gopkg.in/kothar/go-backblaze.v0"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
)

const (
	b2storageType = "b2"
)

type b2Storage struct {
	Options

	ctx context.Context

	cli    *backblaze.B2
	bucket *backblaze.Bucket

	downloadThrottler *iothrottler.IOThrottlerPool
	uploadThrottler   *iothrottler.IOThrottlerPool
}

func (s *b2Storage) GetBlob(ctx context.Context, id blob.ID, offset, length int64) ([]byte, error) {
	fileName := s.getObjectNameString(id)

	attempt := func() (interface{}, error) {
		var fileRange *backblaze.FileRange

		if length > 0 {
			fileRange = &backblaze.FileRange{
				Start: offset,
				End:   offset + length - 1,
			}
		}

		_, r, err := s.bucket.DownloadFileRangeByName(fileName, fileRange)
		if err != nil {
			return nil, err
		}
		defer r.Close() //nolint:errcheck

		throttled, err := s.downloadThrottler.AddReader(r)
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

	v, err := exponentialBackoff(ctx, fmt.Sprintf("GetBlob(%q,%v,%v)", id, offset, length), attempt)
	if err != nil {
		return nil, translateError(err)
	}

	return v.([]byte), nil
}

func (s *b2Storage) resolveFileID(fileName string) (string, error) {
	resp, err := s.bucket.ListFileVersions(fileName, "", 1)
	if err != nil {
		return "", err
	}

	if len(resp.Files) > 0 {
		if resp.Files[0].Name == fileName && resp.Files[0].Action == backblaze.Upload {
			return resp.Files[0].ID, nil
		}
	}

	return "", nil
}

func (s *b2Storage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	fileName := s.getObjectNameString(id)

	attempt := func() (interface{}, error) {
		fileID, err := s.resolveFileID(fileName)
		if err != nil {
			return nil, err
		}

		fi, err := s.bucket.GetFileInfo(fileID)
		if err != nil {
			return nil, err
		}

		return blob.Metadata{
			BlobID:    id,
			Length:    fi.ContentLength,
			Timestamp: time.Unix(0, fi.UploadTimestamp*1e6),
		}, nil
	}

	v, err := exponentialBackoff(ctx, fmt.Sprintf("GetMetadata(%q)", id), attempt)
	if err != nil {
		return blob.Metadata{}, translateError(err)
	}

	return v.(blob.Metadata), nil
}

func translateError(err error) error {
	if b2err, ok := err.(*backblaze.B2Error); ok {
		if b2err.Status == http.StatusNotFound {
			// Normal "not found". That's fine.
			return blob.ErrBlobNotFound
		}

		if b2err.Status == http.StatusBadRequest && b2err.Code == "already_hidden" {
			// Special case when hiding a file that is already hidden. It's basically
			// not found.
			return blob.ErrBlobNotFound
		}
	}

	return err
}

func exponentialBackoff(ctx context.Context, desc string, att retry.AttemptFunc) (interface{}, error) {
	return retry.WithExponentialBackoff(ctx, desc, att, isRetriableError)
}

func isRetriableError(err error) bool {
	if b2err, ok := err.(*backblaze.B2Error); ok {
		switch b2err.Status {
		case http.StatusRequestTimeout:
			return true
		case http.StatusTooManyRequests:
			return true
		case http.StatusInternalServerError:
			return true
		case http.StatusServiceUnavailable:
			return true
		case http.StatusGatewayTimeout:
			return true
		}
	}

	return false
}

func (s *b2Storage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes) error {
	throttled, err := s.uploadThrottler.AddReader(ioutil.NopCloser(data.Reader()))
	if err != nil {
		return err
	}

	progressCallback := blob.ProgressCallback(ctx)
	if progressCallback != nil {
		progressCallback(string(id), 0, int64(data.Length()))
		defer progressCallback(string(id), int64(data.Length()), int64(data.Length()))
	}

	fileName := s.getObjectNameString(id)
	_, err = s.bucket.UploadFile(fileName, nil, throttled)

	return translateError(err)
}

func (s *b2Storage) DeleteBlob(ctx context.Context, id blob.ID) error {
	fileName := s.getObjectNameString(id)
	_, err := s.bucket.HideFile(fileName)
	err = translateError(err)

	if err == blob.ErrBlobNotFound {
		// Deleting failed because it already is deleted? Fine.
		return nil
	}

	return err
}

func (s *b2Storage) getObjectNameString(id blob.ID) string {
	return s.Prefix + string(id)
}

func (s *b2Storage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	const maxFileQuery = 1000

	fullPrefix := s.getObjectNameString(prefix)
	nextFile := ""

	for {
		resp, err := s.bucket.ListFileNamesWithPrefix(nextFile, maxFileQuery, fullPrefix, "")
		if err != nil {
			return err
		}

		for i := range resp.Files {
			f := &resp.Files[i]
			bm := blob.Metadata{
				BlobID:    blob.ID(f.Name[len(s.Prefix):]),
				Length:    f.ContentLength,
				Timestamp: time.Unix(0, f.UploadTimestamp*int64(time.Millisecond)),
			}

			if err := callback(bm); err != nil {
				return err
			}
		}

		nextFile = resp.NextFileName

		if nextFile == "" {
			break
		}
	}

	return nil
}

func (s *b2Storage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   b2storageType,
		Config: &s.Options,
	}
}

func (s *b2Storage) DisplayName() string {
	return fmt.Sprintf("B2: %v", s.BucketName)
}

func (s *b2Storage) Close(ctx context.Context) error {
	return nil
}

func (s *b2Storage) String() string {
	return fmt.Sprintf("b2://%s/%s", s.BucketName, s.Prefix)
}

func toBandwidth(bytesPerSecond int) iothrottler.Bandwidth {
	if bytesPerSecond <= 0 {
		return iothrottler.Unlimited
	}

	return iothrottler.Bandwidth(bytesPerSecond) * iothrottler.BytesPerSecond
}

// New creates new B2-backed storage with specified options:
func New(ctx context.Context, opt *Options) (blob.Storage, error) {
	if opt.BucketName == "" {
		return nil, errors.New("bucket name must be specified")
	}

	cli, err := backblaze.NewB2(backblaze.Credentials{KeyID: opt.KeyID, ApplicationKey: opt.Key})
	if err != nil {
		return nil, errors.Wrap(err, "unable to create client")
	}

	downloadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxDownloadSpeedBytesPerSecond))
	uploadThrottler := iothrottler.NewIOThrottlerPool(toBandwidth(opt.MaxUploadSpeedBytesPerSecond))

	bucket, err := cli.Bucket(opt.BucketName)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open bucket %q", opt.BucketName)
	}

	if bucket == nil {
		return nil, fmt.Errorf("bucket not found: %s", opt.BucketName)
	}

	return &b2Storage{
		Options:           *opt,
		ctx:               ctx,
		cli:               cli,
		bucket:            bucket,
		downloadThrottler: downloadThrottler,
		uploadThrottler:   uploadThrottler,
	}, nil
}

func init() {
	blob.AddSupportedStorage(
		b2storageType,
		func() interface{} {
			return &Options{}
		},
		func(ctx context.Context, o interface{}) (blob.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
