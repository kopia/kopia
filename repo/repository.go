package repo

import (
	"context"
	"time"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/throttling"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/pkg/errors"
)

// Repository exposes public API of Kopia repository, including objects and manifests.
//
//nolint:interfacebloat
type Repository interface {
	OpenObject(ctx context.Context, id object.ID) (object.Reader, error)
	VerifyObject(ctx context.Context, id object.ID) ([]content.ID, error)
	GetManifest(ctx context.Context, id manifest.ID, data any) (*manifest.EntryMetadata, error)
	FindManifests(ctx context.Context, labels map[string]string) ([]*manifest.EntryMetadata, error)
	ContentInfo(ctx context.Context, contentID content.ID) (content.Info, error)
	PrefetchContents(ctx context.Context, contentIDs []content.ID, hint string) []content.ID
	PrefetchObjects(ctx context.Context, objectIDs []object.ID, hint string) ([]content.ID, error)
	Time() time.Time
	ClientOptions() ClientOptions
	NewWriter(ctx context.Context, opt WriteSessionOptions) (context.Context, RepositoryWriter, error)
	UpdateDescription(d string)
	Refresh(ctx context.Context) error
	Close(ctx context.Context) error
}

// ConcatenateOptions describes options for concatenating objects.
type ConcatenateOptions struct {
	Compressor compression.Name
}

// RepositoryWriter provides methods to write to a repository.
type RepositoryWriter interface {
	Repository

	NewObjectWriter(ctx context.Context, opt object.WriterOptions) object.Writer
	ConcatenateObjects(ctx context.Context, objectIDs []object.ID, opt ConcatenateOptions) (object.ID, error)
	PutManifest(ctx context.Context, labels map[string]string, payload any) (manifest.ID, error)
	ReplaceManifests(ctx context.Context, labels map[string]string, payload any) (manifest.ID, error)
	DeleteManifest(ctx context.Context, id manifest.ID) error
	OnSuccessfulFlush(callback RepositoryWriterCallback)
	Flush(ctx context.Context) error
}

// RemoteRetentionPolicy is an interface implemented by repository clients that support remote retention policy.
// when implemented, the repository server will invoke ApplyRetentionPolicy() server-side.
type RemoteRetentionPolicy interface {
	ApplyRetentionPolicy(ctx context.Context, sourcePath string, reallyDelete bool) ([]manifest.ID, error)
}

// RemoteNotifications is an interface implemented by repository clients that support remote notifications.
type RemoteNotifications interface {
	SendNotification(ctx context.Context, templateName string, templateDataJSON []byte, templateDataType int32, severity int32) error
}

// DirectRepository provides additional low-level repository functionality.
//
//nolint:interfacebloat
type DirectRepository interface {
	Repository

	ObjectFormat() format.ObjectFormat
	FormatManager() *format.Manager
	BlobReader() blob.Reader
	BlobVolume() blob.Volume
	ContentReader() content.Reader
	IndexBlobs(ctx context.Context, includeInactive bool) ([]indexblob.Metadata, error)
	NewDirectWriter(ctx context.Context, opt WriteSessionOptions) (context.Context, DirectRepositoryWriter, error)
	UniqueID() []byte
	ConfigFilename() string
	DeriveKey(purpose string, keyLength int) ([]byte, error)
	Token(password string) (string, error)
	Throttler() throttling.SettableThrottler
	DisableIndexRefresh()
}

// DirectRepositoryWriter provides low-level write access to the repository.
type DirectRepositoryWriter interface {
	RepositoryWriter
	DirectRepository
	BlobStorage() blob.Storage
	ContentManager() *content.WriteManager
}

// RepositoryWriterCallback is a hook function invoked before and after each flush.
type RepositoryWriterCallback func(ctx context.Context, w RepositoryWriter) error

// WriteSessionOptions describes options for a write session.
type WriteSessionOptions struct {
	Purpose        string
	FlushOnFailure bool        // whether to flush regardless of write session result.
	OnUpload       func(int64) // function to invoke after completing each upload in the session.
}

// WriteSession executes the provided callback in a repository writer created for the purpose and flushes writes.
func WriteSession(ctx context.Context, r Repository, opt WriteSessionOptions, cb func(ctx context.Context, w RepositoryWriter) error) error {
	ctx, span := tracer.Start(ctx, "WriteSession:"+opt.Purpose)
	defer span.End()

	ctx, w, err := r.NewWriter(ctx, opt)
	if err != nil {
		return errors.Wrap(err, "unable to create writer")
	}

	return handleWriteSessionResult(ctx, w, opt, cb(ctx, w))
}

// DirectWriteSession executes the provided callback in a DirectRepositoryWriter created for the purpose and flushes writes.
func DirectWriteSession(ctx context.Context, r DirectRepository, opt WriteSessionOptions, cb func(ctx context.Context, dw DirectRepositoryWriter) error) error {
	ctx, span := tracer.Start(ctx, "DirectWriteSession:"+opt.Purpose)
	defer span.End()

	ctx, w, err := r.NewDirectWriter(ctx, opt)
	if err != nil {
		return errors.Wrap(err, "unable to create direct writer")
	}

	return handleWriteSessionResult(ctx, w, opt, cb(ctx, w))
}

func handleWriteSessionResult(ctx context.Context, w RepositoryWriter, opt WriteSessionOptions, resultErr error) error {
	defer func() {
		if err := w.Close(ctx); err != nil {
			log(ctx).Errorf("error closing writer: %v", err)
		}
	}()

	if resultErr == nil || opt.FlushOnFailure {
		if err := w.Flush(ctx); err != nil {
			return errors.Wrap(err, "error flushing writer")
		}
	}

	return resultErr
}
