package repo

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

// Repository exposes public API of Kopia repository, including objects and manifests.
type Repository interface {
	OpenObject(ctx context.Context, id object.ID) (object.Reader, error)
	VerifyObject(ctx context.Context, id object.ID) ([]content.ID, error)

	GetManifest(ctx context.Context, id manifest.ID, data interface{}) (*manifest.EntryMetadata, error)
	FindManifests(ctx context.Context, labels map[string]string) ([]*manifest.EntryMetadata, error)

	Time() time.Time
	ClientOptions() ClientOptions

	NewWriter(ctx context.Context, opt WriteSessionOptions) (RepositoryWriter, error)

	UpdateDescription(d string)

	Refresh(ctx context.Context) error
	Close(ctx context.Context) error
}

// RepositoryWriter provides methods to write to a repository.
type RepositoryWriter interface {
	Repository

	NewObjectWriter(ctx context.Context, opt object.WriterOptions) object.Writer
	PutManifest(ctx context.Context, labels map[string]string, payload interface{}) (manifest.ID, error)
	DeleteManifest(ctx context.Context, id manifest.ID) error

	Flush(ctx context.Context) error
}

// DirectRepository provides additional low-level repository functionality.
type DirectRepository interface {
	Repository

	ObjectFormat() object.Format
	BlobReader() blob.Reader
	ContentReader() content.Reader
	IndexBlobReader() content.IndexBlobReader

	NewDirectWriter(ctx context.Context, opt WriteSessionOptions) (DirectRepositoryWriter, error)

	// misc
	UniqueID() []byte
	ConfigFilename() string
	DeriveKey(purpose []byte, keyLength int) []byte
	Token(password string) (string, error)
}

// DirectRepositoryWriter provides low-level write access to the repository.
type DirectRepositoryWriter interface {
	RepositoryWriter
	DirectRepository

	BlobStorage() blob.Storage
	ContentManager() *content.WriteManager
	Upgrade(ctx context.Context) error
}

type directRepositoryParameters struct {
	uniqueID       []byte
	configFile     string
	cachingOptions content.CachingOptions
	cliOpts        ClientOptions
	timeNow        func() time.Time
	formatBlob     *formatBlob
	masterKey      []byte
}

// directRepository is an implementation of repository that directly manipulates underlying storage.
type directRepository struct {
	directRepositoryParameters

	blobs blob.Storage
	cmgr  *content.WriteManager
	omgr  *object.Manager
	mmgr  *manifest.Manager
	sm    *content.SharedManager

	closed chan struct{}
}

// DeriveKey derives encryption key of the provided length from the master key.
func (r *directRepository) DeriveKey(purpose []byte, keyLength int) []byte {
	return deriveKeyFromMasterKey(r.masterKey, r.uniqueID, purpose, keyLength)
}

// ClientOptions returns client options.
func (r *directRepository) ClientOptions() ClientOptions {
	return r.cliOpts
}

// BlobStorage returns the blob storage.
func (r *directRepository) BlobStorage() blob.Storage {
	return r.blobs
}

// ContentManager returns the content manager.
func (r *directRepository) ContentManager() *content.WriteManager {
	return r.cmgr
}

// ConfigFilename returns the name of the configuration file.
func (r *directRepository) ConfigFilename() string {
	return r.configFile
}

// NewObjectWriter creates an object writer.
func (r *directRepository) NewObjectWriter(ctx context.Context, opt object.WriterOptions) object.Writer {
	return r.omgr.NewWriter(ctx, opt)
}

// OpenObject opens the reader for a given object, returns object.ErrNotFound.
func (r *directRepository) OpenObject(ctx context.Context, id object.ID) (object.Reader, error) {
	return object.Open(ctx, r.cmgr, id)
}

// VerifyObject verifies that the given object is stored properly in a repository and returns backing content IDs.
func (r *directRepository) VerifyObject(ctx context.Context, id object.ID) ([]content.ID, error) {
	return object.VerifyObject(ctx, r.cmgr, id)
}

// GetManifest returns the given manifest data and metadata.
func (r *directRepository) GetManifest(ctx context.Context, id manifest.ID, data interface{}) (*manifest.EntryMetadata, error) {
	return r.mmgr.Get(ctx, id, data)
}

// PutManifest saves the given manifest payload with a set of labels.
func (r *directRepository) PutManifest(ctx context.Context, labels map[string]string, payload interface{}) (manifest.ID, error) {
	return r.mmgr.Put(ctx, labels, payload)
}

// FindManifests returns metadata for manifests matching given set of labels.
func (r *directRepository) FindManifests(ctx context.Context, labels map[string]string) ([]*manifest.EntryMetadata, error) {
	return r.mmgr.Find(ctx, labels)
}

// DeleteManifest deletes the manifest with a given ID.
func (r *directRepository) DeleteManifest(ctx context.Context, id manifest.ID) error {
	return r.mmgr.Delete(ctx, id)
}

// ListActiveSessions returns the map of active sessions.
func (r *directRepository) ListActiveSessions(ctx context.Context) (map[content.SessionID]*content.SessionInfo, error) {
	return r.cmgr.ListActiveSessions(ctx)
}

// UpdateDescription updates the description of a connected repository.
func (r *directRepository) UpdateDescription(d string) {
	r.cliOpts.Description = d
}

// NewWriter returns new RepositoryWriter session for repository.
func (r *directRepository) NewWriter(ctx context.Context, opt WriteSessionOptions) (RepositoryWriter, error) {
	return r.NewDirectWriter(ctx, opt)
}

// NewDirectWriter returns new DirectRepositoryWriter session for repository.
func (r *directRepository) NewDirectWriter(ctx context.Context, opt WriteSessionOptions) (DirectRepositoryWriter, error) {
	cmgr := content.NewWriteManager(r.sm, content.SessionOptions{
		SessionUser: r.cliOpts.Username,
		SessionHost: r.cliOpts.Hostname,
		OnUpload:    opt.OnUpload,
	})

	mmgr, err := manifest.NewManager(ctx, cmgr, manifest.ManagerOptions{
		TimeNow: r.timeNow,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error creating manifest manager")
	}

	omgr, err := object.NewObjectManager(ctx, cmgr, r.omgr.Format)
	if err != nil {
		return nil, errors.Wrap(err, "error creating object manager")
	}

	w := &directRepository{
		directRepositoryParameters: r.directRepositoryParameters,
		blobs:                      r.blobs,
		cmgr:                       cmgr,
		omgr:                       omgr,
		mmgr:                       mmgr,
		sm:                         r.sm,
		closed:                     make(chan struct{}),
	}

	return w, nil
}

// Close closes the repository and releases all resources.
func (r *directRepository) Close(ctx context.Context) error {
	select {
	case <-r.closed:
		// already closed
		return nil

	default:
	}

	if err := r.omgr.Close(); err != nil {
		return errors.Wrap(err, "error closing object manager")
	}

	// this will release shared manager and MAY release blob.Store (on last outstanding reference).
	if err := r.cmgr.Close(ctx); err != nil {
		return errors.Wrap(err, "error closing content-addressable storage manager")
	}

	close(r.closed)

	return nil
}

// Flush waits for all in-flight writes to complete.
func (r *directRepository) Flush(ctx context.Context) error {
	if err := r.mmgr.Flush(ctx); err != nil {
		return errors.Wrap(err, "error flushing manifests")
	}

	return r.cmgr.Flush(ctx)
}

// ObjectFormat returns the object format.
func (r *directRepository) ObjectFormat() object.Format {
	return r.omgr.Format
}

// UniqueID returns unique repository ID from which many keys and secrets are derived.
func (r *directRepository) UniqueID() []byte {
	return r.uniqueID
}

// BlobReader returns the blob reader.
func (r *directRepository) BlobReader() blob.Reader {
	return r.blobs
}

// ContentReader returns the content reader.
func (r *directRepository) ContentReader() content.Reader {
	return r.cmgr
}

// IndexBlobReader returns the index blob reader.
func (r *directRepository) IndexBlobReader() content.IndexBlobReader {
	return r.cmgr
}

// Refresh periodically makes external changes visible to repository.
func (r *directRepository) Refresh(ctx context.Context) error {
	_, err := r.cmgr.Refresh(ctx)
	if err != nil {
		return errors.Wrap(err, "error refreshing content index")
	}

	return nil
}

// RefreshPeriodically periodically refreshes the repository to reflect the changes made by other hosts.
func (r *directRepository) RefreshPeriodically(ctx context.Context, interval time.Duration) {
	for {
		select {
		case <-r.closed:
			// stop background refresh when repository is closed
			return

		case <-ctx.Done():
			return

		case <-time.After(interval):
			if err := r.Refresh(ctx); err != nil {
				log(ctx).Errorf("error refreshing repository: %v", err)
			}
		}
	}
}

// Time returns the current local time for the repo.
func (r *directRepository) Time() time.Time {
	return defaultTime(r.timeNow)()
}

// WriteSessionOptions describes options for a write session.
type WriteSessionOptions struct {
	Purpose        string
	FlushOnFailure bool        // whether to flush regardless of write session result.
	OnUpload       func(int64) // function to invoke after completing each upload in the session.
}

// WriteSession executes the provided callback in a repository writer created for the purpose and flushes writes.
func WriteSession(ctx context.Context, r Repository, opt WriteSessionOptions, cb func(w RepositoryWriter) error) error {
	w, err := r.NewWriter(ctx, opt)
	if err != nil {
		return errors.Wrap(err, "unable to create writer")
	}

	return handleWriteSessionResult(ctx, w, opt, cb(w))
}

// DirectWriteSession executes the provided callback in a DirectRepositoryWriter created for the purpose and flushes writes.
func DirectWriteSession(ctx context.Context, r DirectRepository, opt WriteSessionOptions, cb func(dw DirectRepositoryWriter) error) error {
	w, err := r.NewDirectWriter(ctx, opt)
	if err != nil {
		return errors.Wrap(err, "unable to create direct writer")
	}

	return handleWriteSessionResult(ctx, w, opt, cb(w))
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

func defaultTime(f func() time.Time) func() time.Time {
	if f != nil {
		return f
	}

	return clock.Now
}

var _ DirectRepositoryWriter = (*directRepository)(nil)
