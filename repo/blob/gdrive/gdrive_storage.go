// Package gdrive implements Storage based on Google Drive.
package gdrive

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	drive "google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	gdriveStorageType = "gdrive"
	gdriveMimeType    = "application/x-kopia"
	
	uploadChunkSize   = 1 << 20
	uploadContentType = "application/octet-stream"
	maxPrefixLen      = 26
	queryRetryDelay   = 100 * time.Millisecond
	queryRetryMax     = 3

  // googleapi.Field values.
	listIdFields       = "files(name,id)"
	listMetadataFields = "files(name,id,mimeType,size,modifiedTime)"
	getMetadataFields  = "name,id,mimeType,size,modifiedTime"
)

type gdriveStorage struct {
	Options

	client      *drive.Service
	folderId    string
	// Caches blob id -> drive file id associations.
	fileIdCache map[blob.ID]string
}

func (gdrive *gdriveStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	if offset < 0 {
		return blob.ErrInvalidRange
	}

	fileId, err := gdrive.GetFileId(ctx, b)
	if err != nil {
		return err
	}

	get_req := gdrive.client.Files.Get(fileId)
	get_req.Header().Set("Range", toRange(offset, length))
	res, err := get_req.Context(ctx).Download()
	if err != nil {
		return errors.Wrap(translateError(err), "Get blob")
	}
	defer res.Body.Close() //nolint:errcheck

	if length != 0 {
		// nolint:wrapcheck
		if err := iocopy.JustCopy(output, res.Body); err != nil {
			return translateError(err)
		}
	}

	// nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func (gdrive *gdriveStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	fileId, ok := gdrive.fileIdCache[b]
	var file *drive.File
	var err error

	if ok {
		file, err = gdrive.client.Files.Get(fileId).Fields(getMetadataFields).Context(ctx).Do()
		err = translateError(err)
	} else {
		file, err = gdrive.QueryForBlob(ctx, b, listMetadataFields)
	}
	if err != nil {
		return blob.Metadata{}, errors.Wrap(err, "get blob metadata")
	}

	bm, err := parseBlobMetadata(file, b)
	if err != nil {
		return blob.Metadata{}, err
	}

	return bm, nil
}

func (gdrive *gdriveStorage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	switch {
	case opts.HasRetentionOptions():
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "blob-retention")
	}

	fileId, err := gdrive.GetFileId(ctx, b)
	existingFile := true
	if errors.Is(err, blob.ErrBlobNotFound) {
		existingFile = false
	} else if err != nil {
		return err
	}
	if existingFile && opts.DoNotRecreate {
		return blob.ErrBlobAlreadyExists
	}

	var file *drive.File
	mtime := ""

	if !opts.SetModTime.IsZero() {
		mtime = opts.SetModTime.Format(time.RFC3339)
	}

	if !existingFile {
		file, err = gdrive.client.Files.Create(&drive.File{
			Name:         toFileName(b),
			Parents:      []string{gdrive.folderId},
			MimeType:     gdriveMimeType,
			ModifiedTime: mtime,
		}).Media(data.Reader(),
			googleapi.ChunkSize(uploadChunkSize),
			googleapi.ContentType(uploadContentType),
		).Context(ctx).Do()
	} else {
		file, err = gdrive.client.Files.Update(fileId, &drive.File{
			ModifiedTime: mtime,
		}).Media(
			data.Reader(),
			googleapi.ChunkSize(uploadChunkSize),
			googleapi.ContentType(uploadContentType),
		).Context(ctx).Do()
	}
	if err != nil {
		return translateError(err)
	}

	gdrive.fileIdCache[b] = file.Id
	if opts.GetModTime != nil {
		if mtime, err := parseModifiedTime(file); err == nil {
			*opts.GetModTime = mtime
		}
	}

	return nil
}

func (gdrive *gdriveStorage) DeleteBlob(ctx context.Context, b blob.ID) error {
	fileId, err := gdrive.GetFileId(ctx, b)
	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	err = gdrive.client.Files.Delete(fileId).Context(ctx).Do()
	return translateError(err)
}

func (gdrive *gdriveStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	// Tracks blob matches in cache but not returned by API.
	unvisitedIds := make(map[blob.ID]bool)

	consumer := func(files *drive.FileList) error {
		for _, file := range files.Files {
			blobId := toBlobId(file.Name)
			gdrive.fileIdCache[blobId] = file.Id

			if !matchesPrefix(blobId, prefix) {
				return nil
			}

			// Mark blob as visited.
			delete(unvisitedIds, blobId)

			bm, err := parseBlobMetadata(file, blobId)
			if err != nil {
				return err
			}

			if err := callback(bm); err != nil {
				return err
			}
		}

		return nil
	}

	query := fmt.Sprintf("'%s' in parents and mimeType = '%s'", gdrive.folderId, gdriveMimeType)
	if prefix != "" {
		query = fmt.Sprintf("'%s' in parents and name contains '%s' and mimeType = '%s'", gdrive.folderId, capPrefix(prefix), gdriveMimeType)

		// Populate unvisited cache.
		for blobId, _ := range gdrive.fileIdCache {
			if matchesPrefix(blobId, prefix) {
				unvisitedIds[blobId] = true
			}
		}
	}

	err := gdrive.client.Files.List().Q(query).Fields("nextPageToken", listMetadataFields).Pages(ctx, consumer)
	if err != nil {
		return translateError(err)
	}

	// Catch any blobs that the API didn't return.
	if len(unvisitedIds) != 0 {
		for blobId, _ := range unvisitedIds {
			bm, err := gdrive.GetMetadata(ctx, blobId)
			if err != nil {
				return translateError(err)
			}

			if err := callback(bm); err != nil {
				return err
			}
		}
	}

	return nil
}

func (gdrive *gdriveStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   gdriveStorageType,
		Config: &gdrive.Options,
	}
}

func (gdrive *gdriveStorage) DisplayName() string {
	return fmt.Sprintf("Google Drive: %v", gdrive.FolderId)
}

func (gdrive *gdriveStorage) Close(ctx context.Context) error {
	return nil
}

func (gdrive *gdriveStorage) FlushCaches(ctx context.Context) error {
	return nil
}

func (gdrive *gdriveStorage) GetFileId(ctx context.Context, blobId blob.ID) (string, error) {
	_, ok := gdrive.fileIdCache[blobId]
	if !ok {
		_, err := gdrive.QueryForBlob(ctx, blobId, listIdFields)
		if err != nil {
			return "", err
		}
	}

	return gdrive.fileIdCache[blobId], nil
}

func (gdrive *gdriveStorage) QueryForBlob(ctx context.Context, blobId blob.ID, fields googleapi.Field) (*drive.File, error) {

	runQuery := func() (interface{}, error) {
		files, err := gdrive.client.Files.List().Q(fmt.Sprintf("'%s' in parents and name = '%s' and mimeType = '%s'", gdrive.folderId, string(blobId), gdriveMimeType)).Fields(fields).PageSize(1).Context(ctx).Do()
		if err != nil {
			return nil, translateError(err)
		} else if len(files.Files) != 1 {
			return nil, blob.ErrBlobNotFound
		} else {
			return files.Files[0], nil
		}
	}

	f, err := retry.Periodically(ctx, queryRetryDelay, queryRetryMax, fmt.Sprintf("QueryForBlob(%v)", blobId), runQuery, retryNotFound)
	if err != nil {
		return nil, err
	}

	file := f.(*drive.File) //nolint:forcetypeassert
	gdrive.fileIdCache[toBlobId(file.Name)] = file.Id
	return file, nil
}

func matchesPrefix(blobId blob.ID, prefix blob.ID) bool {
	return strings.HasPrefix(string(blobId), string(prefix))
}

// Drive doesn't support prefix match beyond a certain length.
func capPrefix(prefix blob.ID) string {
	if len(prefix) >= maxPrefixLen {
		return string(prefix)[:maxPrefixLen]
	}

	return string(prefix)
}

// Returns a valid HTTP Range header value.
func toRange(offset, length int64) string {
	if length < 0 {
		return fmt.Sprintf("bytes=%d-", offset)
	} else if length == 0 {
		// There's no way to read 0 bytes, so we read 1 byte instead.
		return fmt.Sprintf("bytes=%d-%d", offset, offset)
	} else {
		return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	}
}

func toFileName(blobId blob.ID) string {
	return string(blobId)
}

func toBlobId(fileName string) blob.ID {
	return blob.ID(fileName)
}

func parseBlobMetadata(file *drive.File, blobId blob.ID) (blob.Metadata, error) {
	mtime, err := parseModifiedTime(file)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(err, "error parsing file modified time")
	}

	bm := blob.Metadata{
		BlobID:    blobId,
		Length:    file.Size,
		Timestamp: mtime,
	}

	return bm, nil
}

func parseModifiedTime(file *drive.File) (time.Time, error) {
	return time.Parse(time.RFC3339, file.ModifiedTime)
}

func retryNotFound(err error) bool {
	return errors.Is(err, blob.ErrBlobNotFound)
}

func translateError(err error) error {
	var ae *googleapi.Error

	if errors.As(err, &ae) {
		switch ae.Code {
		case http.StatusNotFound:
			return blob.ErrBlobNotFound
		case http.StatusRequestedRangeNotSatisfiable:
			return blob.ErrInvalidRange
		case http.StatusPreconditionFailed:
			return blob.ErrBlobAlreadyExists
		}
	}

	switch {
	case err == nil:
		return nil
	default:
		return errors.Wrap(err, "unexpected Google Drive error")
	}
}

func tokenSourceFromCredentialsFile(ctx context.Context, fn string, scopes ...string) (oauth2.TokenSource, error) {
	data, err := os.ReadFile(fn) //nolint:gosec
	if err != nil {
		return nil, errors.Wrap(err, "error reading credentials file")
	}

	cfg, err := google.JWTConfigFromJSON(data, scopes...)
	if err != nil {
		return nil, errors.Wrap(err, "google.JWTConfigFromJSON")
	}

	return cfg.TokenSource(ctx), nil
}

func tokenSourceFromCredentialsJSON(ctx context.Context, data json.RawMessage, scopes ...string) (oauth2.TokenSource, error) {
	cfg, err := google.JWTConfigFromJSON([]byte(data), scopes...)
	if err != nil {
		return nil, errors.Wrap(err, "google.JWTConfigFromJSON")
	}

	return cfg.TokenSource(ctx), nil
}

// New creates new Google Cloud Storage-backed storage with specified options:
//
// - the 'FolderId' field is required and all other parameters are optional.
//
// By default the connection reuses credentials managed by (https://cloud.google.com/sdk/),
// but this can be disabled by setting IgnoreDefaultCredentials to true.
func New(ctx context.Context, opt *Options) (blob.Storage, error) {
	var err error
	var ts oauth2.TokenSource

	scope := drive.DriveFileScope
	if opt.ReadOnly {
		scope = drive.DriveReadonlyScope
	}

	if sa := opt.ServiceAccountCredentialJSON; len(sa) > 0 {
		ts, err = tokenSourceFromCredentialsJSON(ctx, sa, scope)
	} else if sa := opt.ServiceAccountCredentialsFile; sa != "" {
		ts, err = tokenSourceFromCredentialsFile(ctx, sa, scope)
	} else {
		ts, err = google.DefaultTokenSource(ctx, scope)
	}

	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize token source")
	}

	hc := oauth2.NewClient(ctx, ts)

	service, err := drive.NewService(ctx, option.WithHTTPClient(hc))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create Drive client")
	}

	if opt.FolderId == "" {
		return nil, errors.New("folder-id must be specified")
	}

	gdrive := &gdriveStorage{
		Options:     *opt,
		client:      service,
		folderId:    opt.FolderId,
		fileIdCache: make(map[blob.ID]string),
	}

	// verify GCS connection is functional by listing blobs in a bucket, which will fail if the bucket
	// does not exist. We list with a prefix that will not exist, to avoid iterating through any objects.
	nonExistentPrefix := fmt.Sprintf("kopia-gdrive-storage-initializing-%v", clock.Now().UnixNano())
	err = gdrive.ListBlobs(ctx, blob.ID(nonExistentPrefix), func(md blob.Metadata) error {
		return nil
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to list from the bucket")
	}

	return retrying.NewWrapper(gdrive), nil
}

func init() {
	blob.AddSupportedStorage(
		gdriveStorageType,
		func() interface{} {
			return &Options{}
		},
		func(ctx context.Context, o interface{}, isCreate bool) (blob.Storage, error) {
			return New(ctx, o.(*Options)) //nolint:forcetypeassert
		})
}
