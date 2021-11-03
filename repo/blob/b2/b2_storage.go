// Package b2 implements Storage based on an Backblaze B2 bucket.
package b2

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/efarrer/iothrottler"
	"github.com/pkg/errors"
	"gopkg.in/kothar/go-backblaze.v0"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
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

func (s *b2Storage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	fileName := s.getObjectNameString(id)

	if offset < 0 {
		return blob.ErrInvalidRange
	}

	output.Reset()

	attempt := func() error {
		var fileRange *backblaze.FileRange

		if length > 0 {
			fileRange = &backblaze.FileRange{
				Start: offset,
				End:   offset + length - 1,
			}
		}

		_, r, err := s.bucket.DownloadFileRangeByName(fileName, fileRange)
		if err != nil {
			return errors.Wrap(err, "DownloadFileRangeByName")
		}
		defer r.Close() //nolint:errcheck

		throttled, err := s.downloadThrottler.AddReader(r)
		if err != nil {
			return errors.Wrap(err, "DownloadFileRangeByName")
		}

		if length == 0 {
			return nil
		}

		// nolint:wrapcheck
		return iocopy.JustCopy(output, throttled)
	}

	if err := attempt(); err != nil {
		return translateError(err)
	}

	// nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func (s *b2Storage) resolveFileID(fileName string) (string, error) {
	resp, err := s.bucket.ListFileVersions(fileName, "", 1)
	if err != nil {
		return "", errors.Wrap(err, "ListFileVersions")
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

	fileID, err := s.resolveFileID(fileName)
	if err != nil {
		return blob.Metadata{}, translateError(err)
	}

	fi, err := s.bucket.GetFileInfo(fileID)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(translateError(err), "GetFileInfo")
	}

	return blob.Metadata{
		BlobID:    id,
		Length:    fi.ContentLength,
		Timestamp: time.Unix(0, fi.UploadTimestamp*1e6),
	}, nil
}

func translateError(err error) error {
	if err == nil {
		return nil
	}

	var b2err *backblaze.B2Error
	if errors.As(err, &b2err) {
		switch b2err.Status {
		case http.StatusNotFound:
			// Normal "not found". That's fine.
			return blob.ErrBlobNotFound

		case http.StatusBadRequest:
			if b2err.Code == "already_hidden" || b2err.Code == "no_such_file" {
				// Special case when hiding a file that is already hidden. It's basically
				// not found.
				return blob.ErrBlobNotFound
			}

			if b2err.Code == "bad_request" && strings.HasPrefix(b2err.Message, "Bad fileId") {
				// returned in GetMetadata() when fileId is not found.
				return blob.ErrBlobNotFound
			}

		case http.StatusRequestedRangeNotSatisfiable:
			return blob.ErrInvalidRange
		}
	}

	return err
}

func (s *b2Storage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes) error {
	throttled, err := s.uploadThrottler.AddReader(io.NopCloser(data.Reader()))
	if err != nil {
		return translateError(err)
	}

	fileName := s.getObjectNameString(id)
	_, err = s.bucket.UploadFile(fileName, nil, throttled)

	return translateError(err)
}

func (s *b2Storage) SetTime(ctx context.Context, b blob.ID, t time.Time) error {
	return blob.ErrSetTimeUnsupported
}

func (s *b2Storage) DeleteBlob(ctx context.Context, id blob.ID) error {
	_, err := s.bucket.HideFile(s.getObjectNameString(id))
	err = translateError(err)

	if errors.Is(err, blob.ErrBlobNotFound) {
		// Deleting failed because it already is deleted? Fine.
		return nil
	}

	return nil
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
			// nolint:wrapcheck
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

func (s *b2Storage) FlushCaches(ctx context.Context) error {
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

// New creates new B2-backed storage with specified options.
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
		return nil, errors.Errorf("bucket not found: %s", opt.BucketName)
	}

	return retrying.NewWrapper(&b2Storage{
		Options:           *opt,
		ctx:               ctx,
		cli:               cli,
		bucket:            bucket,
		downloadThrottler: downloadThrottler,
		uploadThrottler:   uploadThrottler,
	}), nil
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
