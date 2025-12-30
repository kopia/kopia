package repo

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/crypto"
	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/internal/repodiag"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/throttling"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

var tracer = otel.Tracer("kopia/repository")

type immutableDirectRepositoryParameters struct {
	configFile      string
	cachingOptions  content.CachingOptions
	cliOpts         ClientOptions
	timeNow         func() time.Time
	fmgr            *format.Manager
	nextWriterID    *atomic.Int32
	throttler       throttling.SettableThrottler
	metricsRegistry *metrics.Registry
	beforeFlush     []RepositoryWriterCallback
	logManager      *repodiag.LogManager

	*refCountedCloser
}

// RepositoryWriterCallback is a hook function invoked before and after each flush.
type RepositoryWriterCallback func(ctx context.Context, w RepositoryWriter) error

func invokeCallbacks(ctx context.Context, w RepositoryWriter, callbacks []RepositoryWriterCallback) error {
	for _, h := range callbacks {
		if err := h(ctx, w); err != nil {
			return err
		}
	}

	return nil
}

// directRepository is an implementation of repository that directly manipulates underlying storage.
type directRepository struct {
	immutableDirectRepositoryParameters

	blobs blob.Storage
	cmgr  *content.WriteManager
	omgr  *object.Manager
	mmgr  *manifest.Manager
	sm    *content.SharedManager

	afterFlush []RepositoryWriterCallback
}

// DeriveKey derives encryption key of the provided length from the master key.
func (r *directRepository) DeriveKey(purpose string, keyLength int) (derivedKey []byte, err error) {
	if r.cmgr.ContentFormat().SupportsPasswordChange() {
		derivedKey, err = crypto.DeriveKeyFromMasterKey(r.cmgr.ContentFormat().GetMasterKey(), r.UniqueID(), purpose, keyLength)
		if err != nil {
			return nil, errors.Wrap(err, "key derivation error")
		}

		return derivedKey, nil
	}

	// version of kopia <v0.9 had a bug where certain keys were derived directly from
	// the password and not from the random master key. This made it impossible to change
	// password.
	derivedKey, err = crypto.DeriveKeyFromMasterKey(r.fmgr.FormatEncryptionKey(), r.UniqueID(), purpose, keyLength)
	if err != nil {
		return nil, errors.Wrap(err, "key derivation error")
	}

	return derivedKey, nil
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

// ConcatenateOptions describes options for concatenating objects.
type ConcatenateOptions struct {
	Compressor compression.Name
}

// ConcatenateObjects creates a concatenated objects from the provided object IDs.
func (r *directRepository) ConcatenateObjects(ctx context.Context, objectIDs []object.ID, opt ConcatenateOptions) (object.ID, error) {
	//nolint:wrapcheck
	return r.omgr.Concatenate(ctx, objectIDs, opt.Compressor)
}

// DisableIndexRefresh disables index refresh for the duration of the write session.
func (r *directRepository) DisableIndexRefresh() {
	r.cmgr.DisableIndexRefresh()
}

// LogManager returns the log manager.
func (r *directRepository) LogManager() *repodiag.LogManager {
	return r.logManager
}

// OpenObject opens the reader for a given object, returns object.ErrNotFound.
func (r *directRepository) OpenObject(ctx context.Context, id object.ID) (object.Reader, error) {
	//nolint:wrapcheck
	return object.Open(ctx, r.cmgr, id)
}

// VerifyObject verifies that the given object is stored properly in a repository and returns backing content IDs.
func (r *directRepository) VerifyObject(ctx context.Context, id object.ID) ([]content.ID, error) {
	//nolint:wrapcheck
	return object.VerifyObject(ctx, r.cmgr, id)
}

// GetManifest returns the given manifest data and metadata.
func (r *directRepository) GetManifest(ctx context.Context, id manifest.ID, data any) (*manifest.EntryMetadata, error) {
	//nolint:wrapcheck
	return r.mmgr.Get(ctx, id, data)
}

// PutManifest saves the given manifest payload with a set of labels.
func (r *directRepository) PutManifest(ctx context.Context, labels map[string]string, payload any) (manifest.ID, error) {
	//nolint:wrapcheck
	return r.mmgr.Put(ctx, labels, payload)
}

// ReplaceManifests saves the given manifest payload with a set of labels and replaces any previous manifests with the same labels.
func (r *directRepository) ReplaceManifests(ctx context.Context, labels map[string]string, payload any) (manifest.ID, error) {
	return replaceManifestsHelper(ctx, r, labels, payload)
}

// FindManifests returns metadata for manifests matching given set of labels.
func (r *directRepository) FindManifests(ctx context.Context, labels map[string]string) ([]*manifest.EntryMetadata, error) {
	//nolint:wrapcheck
	return r.mmgr.Find(ctx, labels)
}

// DeleteManifest deletes the manifest with a given ID.
func (r *directRepository) DeleteManifest(ctx context.Context, id manifest.ID) error {
	//nolint:wrapcheck
	return r.mmgr.Delete(ctx, id)
}

// PrefetchContents brings the requested objects into the cache.
func (r *directRepository) PrefetchContents(ctx context.Context, contentIDs []content.ID, hint string) []content.ID {
	return r.cmgr.PrefetchContents(ctx, contentIDs, hint)
}

// PrefetchObjects brings the requested objects into the cache.
func (r *directRepository) PrefetchObjects(ctx context.Context, objectIDs []object.ID, hint string) ([]content.ID, error) {
	//nolint:wrapcheck
	return object.PrefetchBackingContents(ctx, r.cmgr, objectIDs, hint)
}

// ListActiveSessions returns the map of active sessions.
func (r *directRepository) ListActiveSessions(ctx context.Context) (map[content.SessionID]*content.SessionInfo, error) {
	//nolint:wrapcheck
	return r.cmgr.ListActiveSessions(ctx)
}

// ContentInfo gets the information about particular content.
func (r *directRepository) ContentInfo(ctx context.Context, contentID content.ID) (content.Info, error) {
	//nolint:wrapcheck
	return r.cmgr.ContentInfo(ctx, contentID)
}

// UpdateDescription updates the description of a connected repository.
func (r *directRepository) UpdateDescription(d string) {
	r.cliOpts.Description = d
}

// NewWriter returns new RepositoryWriter session for repository.
func (r *directRepository) NewWriter(ctx context.Context, opt WriteSessionOptions) (context.Context, RepositoryWriter, error) {
	return r.NewDirectWriter(ctx, opt)
}

// NewDirectWriter returns new DirectRepositoryWriter session for repository.
func (r *directRepository) NewDirectWriter(ctx context.Context, opt WriteSessionOptions) (context.Context, DirectRepositoryWriter, error) {
	writeManagerID := fmt.Sprintf("writer-%v:%v", r.nextWriterID.Add(1), opt.Purpose)

	cmgr := content.NewWriteManager(ctx, r.sm, content.SessionOptions{
		SessionUser: r.cliOpts.Username,
		SessionHost: r.cliOpts.Hostname,
		OnUpload:    opt.OnUpload,
	}, writeManagerID)

	mmgr, err := manifest.NewManager(ctx, cmgr, manifest.ManagerOptions{
		TimeNow: r.timeNow,
	}, r.metricsRegistry)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error creating manifest manager")
	}

	omgr, err := object.NewObjectManager(ctx, cmgr, r.omgr.Format, r.metricsRegistry)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error creating object manager")
	}

	w := &directRepository{
		immutableDirectRepositoryParameters: r.immutableDirectRepositoryParameters,
		blobs:                               r.blobs,
		cmgr:                                cmgr,
		omgr:                                omgr,
		mmgr:                                mmgr,
		sm:                                  r.sm,
	}

	w.addRef()

	return ctx, w, nil
}

// Flush waits for all in-flight writes to complete.
func (r *directRepository) Flush(ctx context.Context) error {
	if err := invokeCallbacks(ctx, r, r.beforeFlush); err != nil {
		return errors.Wrap(err, "before flush")
	}

	if err := r.mmgr.Flush(ctx); err != nil {
		return errors.Wrap(err, "error flushing manifests")
	}

	if err := r.cmgr.Flush(ctx); err != nil {
		return errors.Wrap(err, "error flushing contents")
	}

	if err := invokeCallbacks(ctx, r, r.afterFlush); err != nil {
		return errors.Wrap(err, "after flush")
	}

	return nil
}

// Metrics provides access to metrics registry.
func (r *directRepository) Metrics() *metrics.Registry {
	return r.metricsRegistry
}

// ObjectFormat returns the object format.
func (r *directRepository) ObjectFormat() format.ObjectFormat {
	return r.omgr.Format
}

// UniqueID returns unique repository ID from which many keys and secrets are derived.
func (r *directRepository) UniqueID() []byte {
	return r.fmgr.UniqueID()
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
func (r *directRepository) IndexBlobs(ctx context.Context, includeInactive bool) ([]indexblob.Metadata, error) {
	//nolint:wrapcheck
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

// FormatManager returns the format manager.
func (r *directRepository) FormatManager() *format.Manager {
	return r.fmgr
}

// OnSuccessfulFlush registers the provided callback to be invoked after flush succeeds.
func (r *directRepository) OnSuccessfulFlush(callback RepositoryWriterCallback) {
	r.afterFlush = append(r.afterFlush, callback)
}

// replaceManifestsHelper is a helper that deletes all manifests matching provided labels and replaces them with the provided one.
func replaceManifestsHelper(ctx context.Context, rep RepositoryWriter, labels map[string]string, payload any) (manifest.ID, error) {
	const minReplaceManifestTimeDelta = 100 * time.Millisecond

	md, err := rep.FindManifests(ctx, labels)
	if err != nil {
		return "", errors.Wrap(err, "unable to load manifests")
	}

	for _, em := range md {
		// when replacing a manifest, make sure at least minimal amount of time passes by sleeping for few milliseconds
		// on Windows, the clock does not always advance when measured in quick succession leading to flaky tests.
		age := rep.Time().Sub(em.ModTime)
		if age < minReplaceManifestTimeDelta {
			time.Sleep(minReplaceManifestTimeDelta)
		}

		if err := rep.DeleteManifest(ctx, em.ID); err != nil {
			return "", errors.Wrap(err, "unable to delete previous manifest")
		}
	}

	//nolint:wrapcheck
	return rep.PutManifest(ctx, labels, payload)
}

func defaultTime(f func() time.Time) func() time.Time {
	if f != nil {
		return f
	}

	return clock.Now
}

var _ DirectRepositoryWriter = (*directRepository)(nil)
