package repo

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/feature"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/throttling"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

var tracer = otel.Tracer("kopia/repository")

// Repository exposes public API of Kopia repository, including objects and manifests.
type Repository interface {
	OpenObject(ctx context.Context, id object.ID) (object.Reader, error)
	VerifyObject(ctx context.Context, id object.ID) ([]content.ID, error)
	GetManifest(ctx context.Context, id manifest.ID, data interface{}) (*manifest.EntryMetadata, error)
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

// RepositoryWriter provides methods to write to a repository.
type RepositoryWriter interface {
	Repository

	NewObjectWriter(ctx context.Context, opt object.WriterOptions) object.Writer
	ConcatenateObjects(ctx context.Context, objectIDs []object.ID) (object.ID, error)
	PutManifest(ctx context.Context, labels map[string]string, payload interface{}) (manifest.ID, error)
	DeleteManifest(ctx context.Context, id manifest.ID) error
	Flush(ctx context.Context) error
}

// DirectRepository provides additional low-level repository functionality.
type DirectRepository interface {
	Repository

	ObjectFormat() object.Format
	BlobCfg() content.BlobCfgBlob
	BlobReader() blob.Reader
	BlobVolume() blob.Volume
	ContentReader() content.Reader
	IndexBlobs(ctx context.Context, includeInactive bool) ([]content.IndexBlobInfo, error)
	NewDirectWriter(ctx context.Context, opt WriteSessionOptions) (context.Context, DirectRepositoryWriter, error)
	AlsoLogToContentLog(ctx context.Context) context.Context
	UniqueID() []byte
	ConfigFilename() string
	DeriveKey(purpose []byte, keyLength int) []byte
	Token(password string) (string, error)
	Throttler() throttling.SettableThrottler
	RequiredFeatures() ([]feature.Required, error)
	DisableIndexRefresh()
}

// DirectRepositoryWriter provides low-level write access to the repository.
type DirectRepositoryWriter interface {
	RepositoryWriter
	DirectRepository
	BlobStorage() blob.Storage
	ContentManager() *content.WriteManager
	SetParameters(ctx context.Context, m content.MutableParameters, blobcfg content.BlobCfgBlob, requiredFeatures []feature.Required) error
	ChangePassword(ctx context.Context, newPassword string) error
	GetUpgradeLockIntent(ctx context.Context) (*UpgradeLockIntent, error)
	SetUpgradeLockIntent(ctx context.Context, l UpgradeLockIntent) (*UpgradeLockIntent, error)
	CommitUpgrade(ctx context.Context) error
	RollbackUpgrade(ctx context.Context) error
}

type directRepositoryParameters struct {
	uniqueID            []byte
	configFile          string
	cachingOptions      content.CachingOptions
	cliOpts             ClientOptions
	timeNow             func() time.Time
	formatBlob          *formatBlob
	blobCfgBlob         content.BlobCfgBlob
	formatEncryptionKey []byte
	nextWriterID        *int32
	throttler           throttling.SettableThrottler
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
	if r.cmgr.ContentFormat().SupportsPasswordChange() {
		return deriveKeyFromMasterKey(r.cmgr.ContentFormat().GetMasterKey(), r.uniqueID, purpose, keyLength)
	}

	// version of kopia <v0.9 had a bug where certain keys were derived directly from
	// the password and not from the random master key. This made it impossible to change
	// password.
	return deriveKeyFromMasterKey(r.formatEncryptionKey, r.uniqueID, purpose, keyLength)
}

// ClientOptions returns client options.
func (r *directRepository) ClientOptions() ClientOptions {
	return r.cliOpts
}

// BlobStorage returns the blob storage.
func (r *directRepository) BlobStorage() blob.Storage {
	return r.blobs
}

// Throttler returns the blob storage throttler.
func (r *directRepository) Throttler() throttling.SettableThrottler {
	return r.throttler
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

// ConcatenateObjects creates a concatenated objects from the provided object IDs.
func (r *directRepository) ConcatenateObjects(ctx context.Context, objectIDs []object.ID) (object.ID, error) {
	// nolint:wrapcheck
	return r.omgr.Concatenate(ctx, objectIDs)
}

// DisableIndexRefresh disables index refresh for the duration of the write session.
func (r *directRepository) DisableIndexRefresh() {
	r.cmgr.DisableIndexRefresh()
}

// OpenObject opens the reader for a given object, returns object.ErrNotFound.
func (r *directRepository) OpenObject(ctx context.Context, id object.ID) (object.Reader, error) {
	// nolint:wrapcheck
	return object.Open(ctx, r.cmgr, id)
}

// VerifyObject verifies that the given object is stored properly in a repository and returns backing content IDs.
func (r *directRepository) VerifyObject(ctx context.Context, id object.ID) ([]content.ID, error) {
	// nolint:wrapcheck
	return object.VerifyObject(ctx, r.cmgr, id)
}

// GetManifest returns the given manifest data and metadata.
func (r *directRepository) GetManifest(ctx context.Context, id manifest.ID, data interface{}) (*manifest.EntryMetadata, error) {
	// nolint:wrapcheck
	return r.mmgr.Get(ctx, id, data)
}

// PutManifest saves the given manifest payload with a set of labels.
func (r *directRepository) PutManifest(ctx context.Context, labels map[string]string, payload interface{}) (manifest.ID, error) {
	// nolint:wrapcheck
	return r.mmgr.Put(ctx, labels, payload)
}

// FindManifests returns metadata for manifests matching given set of labels.
func (r *directRepository) FindManifests(ctx context.Context, labels map[string]string) ([]*manifest.EntryMetadata, error) {
	// nolint:wrapcheck
	return r.mmgr.Find(ctx, labels)
}

// DeleteManifest deletes the manifest with a given ID.
func (r *directRepository) DeleteManifest(ctx context.Context, id manifest.ID) error {
	// nolint:wrapcheck
	return r.mmgr.Delete(ctx, id)
}

// PrefetchContents brings the requested objects into the cache.
func (r *directRepository) PrefetchContents(ctx context.Context, contentIDs []content.ID, hint string) []content.ID {
	return r.cmgr.PrefetchContents(ctx, contentIDs, hint)
}

// PrefetchObjects brings the requested objects into the cache.
func (r *directRepository) PrefetchObjects(ctx context.Context, objectIDs []object.ID, hint string) ([]content.ID, error) {
	// nolint:wrapcheck
	return object.PrefetchBackingContents(ctx, r.cmgr, objectIDs, hint)
}

// ListActiveSessions returns the map of active sessions.
func (r *directRepository) ListActiveSessions(ctx context.Context) (map[content.SessionID]*content.SessionInfo, error) {
	// nolint:wrapcheck
	return r.cmgr.ListActiveSessions(ctx)
}

// ContentInfo gets the information about particular content.
func (r *directRepository) ContentInfo(ctx context.Context, contentID content.ID) (content.Info, error) {
	// nolint:wrapcheck
	return r.cmgr.ContentInfo(ctx, contentID)
}

// UpdateDescription updates the description of a connected repository.
func (r *directRepository) UpdateDescription(d string) {
	r.cliOpts.Description = d
}

// AlsoLogToContentLog returns a context that causes all logs to also be sent to content log.
func (r *directRepository) AlsoLogToContentLog(ctx context.Context) context.Context {
	return r.sm.AlsoLogToContentLog(ctx)
}

// NewWriter returns new RepositoryWriter session for repository.
func (r *directRepository) NewWriter(ctx context.Context, opt WriteSessionOptions) (context.Context, RepositoryWriter, error) {
	return r.NewDirectWriter(ctx, opt)
}

// NewDirectWriter returns new DirectRepositoryWriter session for repository.
func (r *directRepository) NewDirectWriter(ctx context.Context, opt WriteSessionOptions) (context.Context, DirectRepositoryWriter, error) {
	writeManagerID := fmt.Sprintf("writer-%v:%v", atomic.AddInt32(r.nextWriterID, 1), opt.Purpose)

	cmgr := content.NewWriteManager(ctx, r.sm, content.SessionOptions{
		SessionUser: r.cliOpts.Username,
		SessionHost: r.cliOpts.Hostname,
		OnUpload:    opt.OnUpload,
	}, writeManagerID)

	mmgr, err := manifest.NewManager(ctx, cmgr, manifest.ManagerOptions{
		TimeNow: r.timeNow,
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "error creating manifest manager")
	}

	omgr, err := object.NewObjectManager(ctx, cmgr, r.omgr.Format)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error creating object manager")
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

	return ctx, w, nil
}

// Close closes the repository and releases all resources.
func (r *directRepository) Close(ctx context.Context) error {
	select {
	case <-r.closed:
		// already closed
		return nil

	default:
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

	return errors.Wrap(r.cmgr.Flush(ctx), "error flushing contents")
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

// BlobVolume returns the blob volume interface.
func (r *directRepository) BlobVolume() blob.Volume {
	return r.blobs
}

// ContentReader returns the content reader.
func (r *directRepository) ContentReader() content.Reader {
	return r.cmgr
}

// IndexBlobs returns the index blobs in use.
func (r *directRepository) IndexBlobs(ctx context.Context, includeInactive bool) ([]content.IndexBlobInfo, error) {
	// nolint:wrapcheck
	return r.cmgr.IndexBlobs(ctx, includeInactive)
}

// Refresh makes external changes visible to repository.
func (r *directRepository) Refresh(ctx context.Context) error {
	return errors.Wrap(r.cmgr.Refresh(ctx), "error refreshing content index")
}

// Time returns the current local time for the repo.
func (r *directRepository) Time() time.Time {
	return defaultTime(r.timeNow)()
}

func (r *directRepository) BlobCfg() content.BlobCfgBlob {
	return r.directRepositoryParameters.blobCfgBlob
}

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

func defaultTime(f func() time.Time) func() time.Time {
	if f != nil {
		return f
	}

	return clock.Now
}

var _ DirectRepositoryWriter = (*directRepository)(nil)
