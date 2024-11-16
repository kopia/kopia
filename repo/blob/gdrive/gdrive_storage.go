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
	"github.com/kopia/kopia/repo/logging"
)

const (
	gdriveStorageType = "gdrive"
	blobMimeType      = "application/x-kopia"

	uploadChunkSize   = 1 << 20
	uploadContentType = "application/octet-stream"
	maxPrefixLen      = 26
	queryRetryDelay   = 100 * time.Millisecond
	queryRetryMax     = 3

	// googleapi.Field values.
	metadataFields     = "name,id,mimeType,size,modifiedTime"
	listIDFields       = "files(name,id)"
	listMetadataFields = "files(name,id,mimeType,size,modifiedTime)"
)

var log = logging.Module("gdrive")

type gdriveStorage struct {
	Options
	blob.DefaultProviderImplementation

	client      *drive.FilesService
	about       *drive.AboutService
	folderID    string
	fileIDCache *fileIDCache
}

func (gdrive *gdriveStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	req := gdrive.about.Get().Fields("storageQuota")

	res, err := req.Context(ctx).Do()
	if err != nil {
		return blob.Capacity{}, errors.Wrap(err, "get about in GetCapacity()")
	}

	q := res.StorageQuota
	if q.Limit == 0 {
		// If Limit is unset then the drive has no size limit.
		return blob.Capacity{}, blob.ErrNotAVolume
	}

	return blob.Capacity{
		SizeB: uint64(q.Limit),                   //nolint:gosec
		FreeB: uint64(q.Limit) - uint64(q.Usage), //nolint:gosec
	}, nil
}

func (gdrive *gdriveStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	if offset < 0 {
		return blob.ErrInvalidRange
	}

	fileID, err := gdrive.getFileID(ctx, b)
	if err != nil {
		return errors.Wrapf(err, "get file id in GetBlob(%s)", b)
	}

	req := gdrive.client.Get(fileID).SupportsAllDrives(true)
	req.Header().Set("Range", toRange(offset, length))

	res, err := req.Context(ctx).Download()
	if err != nil {
		return errors.Wrapf(translateError(err), "Get in GetBlob(%s)", b)
	}
	defer res.Body.Close() //nolint:errcheck

	if length != 0 {
		if err := iocopy.JustCopy(output, res.Body); err != nil {
			return errors.Wrapf(translateError(err), "Reading blob in GetBlob(%s)", b)
		}
	}

	return blob.EnsureLengthExactly(output.Length(), length) //nolint:wrapcheck
}

func (gdrive *gdriveStorage) GetMetadata(ctx context.Context, blobID blob.ID) (blob.Metadata, error) {
	f, err := gdrive.fileIDCache.Lookup(blobID, func(entry *cacheEntry) (interface{}, error) {
		if entry.FileID != "" {
			return &drive.File{
				Id: entry.FileID,
			}, nil
		}

		file, err := gdrive.tryGetFileByBlobID(ctx, blobID, listMetadataFields)
		if err != nil {
			return "", err
		}

		entry.FileID = file.Id

		return file, err
	})
	if err != nil {
		return blob.Metadata{}, errors.Wrapf(err, "get file by blob id in GetMetadata(%s)", blobID)
	}

	file := f.(*drive.File) //nolint:forcetypeassert

	if file.Size == 0 { // Need to retrieve the rest of metadata fields.
		file, err = gdrive.getFileByFileID(ctx, file.Id, metadataFields)
		if err != nil {
			return blob.Metadata{}, errors.Wrapf(err, "get file by file id in GetMetadata(%s)", blobID)
		}
	}

	bm, err := parseBlobMetadata(file, blobID)
	if err != nil {
		return blob.Metadata{}, err
	}

	return bm, nil
}

func (gdrive *gdriveStorage) PutBlob(ctx context.Context, blobID blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	if opts.HasRetentionOptions() {
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "blob-retention")
	}

	_, err := gdrive.fileIDCache.Lookup(blobID, func(entry *cacheEntry) (interface{}, error) {
		fileID, err := gdrive.getFileIDWithCache(ctx, entry)
		existingFile := true

		if errors.Is(err, blob.ErrBlobNotFound) {
			existingFile = false
		} else if err != nil {
			return nil, errors.Wrapf(err, "get file id in PutBlob(%s)", blobID)
		}

		if existingFile && opts.DoNotRecreate {
			return nil, blob.ErrBlobAlreadyExists
		}

		var file *drive.File

		mtime := ""

		if !opts.SetModTime.IsZero() {
			mtime = opts.SetModTime.Format(time.RFC3339)
		}

		if !existingFile {
			file, err = gdrive.client.Create(&drive.File{
				Name:         toFileName(blobID),
				Parents:      []string{gdrive.folderID},
				MimeType:     blobMimeType,
				ModifiedTime: mtime,
			}).
				SupportsAllDrives(true).
				Fields(metadataFields).
				Media(data.Reader(),
					googleapi.ChunkSize(uploadChunkSize),
					googleapi.ContentType(uploadContentType),
				).
				Context(ctx).
				Do()
			if err != nil {
				return nil, errors.Wrapf(translateError(err), "Create in PutBlob(%s)", blobID)
			}

			entry.FileID = file.Id
			gdrive.fileIDCache.RecordBlobChange(blobID, file.Id)
		} else {
			file, err = gdrive.client.Update(fileID, &drive.File{
				ModifiedTime: mtime,
			}).
				SupportsAllDrives(true).
				Fields(metadataFields).
				Media(
					data.Reader(),
					googleapi.ChunkSize(uploadChunkSize),
					googleapi.ContentType(uploadContentType),
				).
				Context(ctx).
				Do()
			if err != nil {
				return nil, errors.Wrapf(translateError(err), "Update in PutBlob(%s)", blobID)
			}
		}

		if opts.GetModTime != nil {
			if modTime, err := parseModifiedTime(file); err == nil {
				*opts.GetModTime = modTime
			}
		}

		return nil, nil
	})

	return err
}

func (gdrive *gdriveStorage) DeleteBlob(ctx context.Context, blobID blob.ID) error {
	_, err := gdrive.fileIDCache.Lookup(blobID, func(entry *cacheEntry) (interface{}, error) {
		handleError := func(err error) error {
			if errors.Is(err, blob.ErrBlobNotFound) {
				log(ctx).Warnf("Trying to non-existent DeleteBlob(%s)", blobID)

				entry.FileID = ""

				return nil
			}

			if err != nil {
				return errors.Wrapf(err, "DeleteBlob(%s)", blobID)
			}

			return nil
		}

		fileID, err := gdrive.getFileIDWithCache(ctx, entry)
		if err != nil {
			return nil, handleError(err)
		}

		err = gdrive.client.Delete(fileID).SupportsAllDrives(true).Context(ctx).Do()
		if err != nil {
			return nil, handleError(translateError(err))
		}

		entry.FileID = ""

		gdrive.fileIDCache.RecordBlobChange(blobID, "")

		return nil, nil
	})

	return err
}

func (gdrive *gdriveStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	// Tracks blob matches in cache but not returned by API.
	unvisitedIDs := make(map[blob.ID]bool)

	consumer := func(files *drive.FileList) error {
		for _, file := range files.Files {
			blobID := toblobID(file.Name)
			gdrive.fileIDCache.BlindPut(blobID, file.Id)

			if !matchesPrefix(blobID, prefix) {
				return nil
			}

			// Mark blob as visited.
			delete(unvisitedIDs, blobID)

			bm, err := parseBlobMetadata(file, blobID)
			if err != nil {
				return err
			}

			if err := callback(bm); err != nil {
				return err
			}
		}

		return nil
	}

	query := fmt.Sprintf("'%s' in parents and mimeType = '%s' and trashed = false", gdrive.folderID, blobMimeType)
	if prefix != "" {
		// Drive API uses `contains` operator for prefix matches: https://developers.google.com/drive/api/v3/reference/query-ref
		query = fmt.Sprintf("'%s' in parents and name contains '%s' and mimeType = '%s'", gdrive.folderID, capPrefix(prefix), blobMimeType)

		// Populate unvisited cache.
		gdrive.fileIDCache.VisitBlobChanges(func(blobID blob.ID, fileID string) {
			if matchesPrefix(blobID, prefix) {
				if fileID != "" {
					unvisitedIDs[blobID] = true
				} else {
					delete(unvisitedIDs, blobID)
				}
			}
		})
	}

	err := gdrive.client.List().SupportsAllDrives(true).IncludeItemsFromAllDrives(true).Q(query).Fields("nextPageToken", listMetadataFields).Pages(ctx, consumer)
	if err != nil {
		return errors.Wrapf(translateError(err), "List in ListBlobs(%s)", prefix)
	}

	// Catch any blobs that the API didn't return.
	if len(unvisitedIDs) != 0 {
		for blobID := range unvisitedIDs {
			bm, err := gdrive.GetMetadata(ctx, blobID)
			if err != nil {
				return errors.Wrapf(translateError(err), "GetMetadata in ListBlobs(%s)", prefix)
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
	return fmt.Sprintf("Google Drive: %v", gdrive.folderID)
}

func (gdrive *gdriveStorage) FlushCaches(ctx context.Context) error {
	gdrive.fileIDCache.Clear()
	return nil
}

func (gdrive *gdriveStorage) getFileID(ctx context.Context, blobID blob.ID) (string, error) {
	fileID, err := gdrive.fileIDCache.Lookup(blobID, func(entry *cacheEntry) (interface{}, error) {
		fileID, err := gdrive.getFileIDWithCache(ctx, entry)
		return fileID, err
	})

	return fileID.(string), err //nolint:forcetypeassert
}

// Get fileID for a blob with the given cache entry.
// If the fileID association is not cached, update the cache entry with an association.
func (gdrive *gdriveStorage) getFileIDWithCache(ctx context.Context, entry *cacheEntry) (string, error) {
	if entry.FileID != "" {
		return entry.FileID, nil
	}

	file, err := gdrive.tryGetFileByBlobID(ctx, entry.BlobID, listIDFields)
	if err != nil {
		return "", err
	}

	entry.FileID = file.Id

	return file.Id, err
}

// Try getFileByBlobID with periodic backoff.
func (gdrive *gdriveStorage) tryGetFileByBlobID(ctx context.Context, blobID blob.ID, fields googleapi.Field) (*drive.File, error) {
	return retry.Periodically(ctx, queryRetryDelay, queryRetryMax, fmt.Sprintf("getFileIDByblobID(%v)", blobID), func() (*drive.File, error) {
		return gdrive.getFileByBlobID(ctx, blobID, fields)
	}, retryNotFound)
}

func (gdrive *gdriveStorage) getFileByFileID(ctx context.Context, fileID string, fields googleapi.Field) (*drive.File, error) {
	file, err := gdrive.client.Get(fileID).SupportsAllDrives(true).Fields(fields).Context(ctx).Do()

	return file, translateError(err)
}

func (gdrive *gdriveStorage) getFileByBlobID(ctx context.Context, blobID blob.ID, fields googleapi.Field) (*drive.File, error) {
	files, err := gdrive.client.List().
		SupportsAllDrives(true).
		IncludeItemsFromAllDrives(true).
		Q(fmt.Sprintf("'%s' in parents and name = '%s' and mimeType = '%s' and trashed = false", gdrive.folderID, toFileName(blobID), blobMimeType)).
		Fields(fields).
		PageSize(2). //nolint:mnd
		Context(ctx).
		Do()

	switch {
	case err != nil:
		return nil, translateError(err)
	case len(files.Files) == 0:
		return nil, errors.Wrapf(blob.ErrBlobNotFound, "No results found for blob id (%s)", blobID)
	case len(files.Files) > 1:
		return nil, errors.Errorf("Multiple files found for blob id (%s)", blobID)
	default:
		return files.Files[0], nil
	}
}

func matchesPrefix(blobID, prefix blob.ID) bool {
	return strings.HasPrefix(string(blobID), string(prefix))
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
	switch {
	case offset == 0 && length < 0:
		return ""
	case length < 0:
		return fmt.Sprintf("bytes=%d-", offset)
	case length == 0:
		// There's no way to read 0 bytes, so we read 1 byte instead.
		return fmt.Sprintf("bytes=%d-%d", offset, offset)
	default:
		return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	}
}

func toFileName(blobID blob.ID) string {
	return string(blobID)
}

func toblobID(fileName string) blob.ID {
	return blob.ID(fileName)
}

func parseBlobMetadata(file *drive.File, blobID blob.ID) (blob.Metadata, error) {
	mtime, err := parseModifiedTime(file)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(err, "error parsing file modified time")
	}

	bm := blob.Metadata{
		BlobID:    blobID,
		Length:    file.Size,
		Timestamp: mtime,
	}

	return bm, nil
}

func parseModifiedTime(file *drive.File) (time.Time, error) {
	return time.Parse(time.RFC3339, file.ModifiedTime) //nolint:wrapcheck
}

func retryNotFound(err error) bool {
	return errors.Is(err, blob.ErrBlobNotFound)
}

func translateError(err error) error {
	var ae *googleapi.Error

	if errors.As(err, &ae) {
		switch ae.Code {
		case http.StatusNotFound:
			return errors.WithMessagef(blob.ErrBlobNotFound, "%v", ae)
		case http.StatusRequestedRangeNotSatisfiable:
			return errors.WithMessagef(blob.ErrInvalidRange, "%v", ae)
		case http.StatusPreconditionFailed:
			return errors.WithMessagef(blob.ErrBlobAlreadyExists, "%v", ae)
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

// CreateDriveService creates a new Google Drive service, which encapsulates multiple clients
// used to access different Google Drive functionality.
// Exported for tests only.
func CreateDriveService(ctx context.Context, opt *Options) (*drive.Service, error) {
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

	return service, nil
}

// New creates new Google Drive-backed storage with specified options:
//
// - the 'folderID' field is required and all other parameters are optional.
//
// By default the connection reuses credentials managed by (https://cloud.google.com/sdk/),
// but this can be disabled by setting IgnoreDefaultCredentials to true.
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
	_ = isCreate

	if opt.FolderID == "" {
		return nil, errors.New("folder-id must be specified")
	}

	service, err := CreateDriveService(ctx, opt)
	if err != nil {
		return nil, err
	}

	gdrive := &gdriveStorage{
		Options:     *opt,
		client:      service.Files,
		about:       service.About,
		folderID:    opt.FolderID,
		fileIDCache: newFileIDCache(),
	}

	// verify Drive connection is functional by listing blobs in a bucket, which will fail if the bucket
	// does not exist. We list with a prefix that will not exist, to avoid iterating through any objects.
	nonExistentPrefix := fmt.Sprintf("kopia-gdrive-storage-initializing-%v", clock.Now().UnixNano())

	err = gdrive.ListBlobs(ctx, blob.ID(nonExistentPrefix), func(_ blob.Metadata) error {
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to list from the folder")
	}

	return retrying.NewWrapper(gdrive), nil
}

func init() {
	blob.AddSupportedStorage(gdriveStorageType, Options{}, New)
}
