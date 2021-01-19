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

// Reader provides methods to read from a repository.
type Reader interface {
	OpenObject(ctx context.Context, id object.ID) (object.Reader, error)
	VerifyObject(ctx context.Context, id object.ID) ([]content.ID, error)

	GetManifest(ctx context.Context, id manifest.ID, data interface{}) (*manifest.EntryMetadata, error)
	FindManifests(ctx context.Context, labels map[string]string) ([]*manifest.EntryMetadata, error)

	Time() time.Time
	ClientOptions() ClientOptions
}

// Writer provides methods to write to a repository.
type Writer interface {
	Reader

	NewObjectWriter(ctx context.Context, opt object.WriterOptions) object.Writer
	PutManifest(ctx context.Context, labels map[string]string, payload interface{}) (manifest.ID, error)
	DeleteManifest(ctx context.Context, id manifest.ID) error

	Flush(ctx context.Context) error
}

// Repository exposes public API of Kopia repository, including objects and manifests.
type Repository interface {
	Reader
	Writer

	UpdateDescription(d string)

	Refresh(ctx context.Context) error
	Close(ctx context.Context) error
}

// DirectRepository is an implementation of repository that directly manipulates underlying storage.
type DirectRepository struct {
	Blobs     blob.Storage
	Content   *content.WriteManager
	Objects   *object.Manager
	Manifests *manifest.Manager
	Cache     content.CachingOptions
	UniqueID  []byte

	sharedContentManager *content.SharedManager

	ConfigFile string

	cliOpts ClientOptions

	timeNow    func() time.Time
	formatBlob *formatBlob
	masterKey  []byte

	closed chan struct{}
}

// DeriveKey derives encryption key of the provided length from the master key.
func (r *DirectRepository) DeriveKey(purpose []byte, keyLength int) []byte {
	return deriveKeyFromMasterKey(r.masterKey, r.UniqueID, purpose, keyLength)
}

// ClientOptions returns client options.
func (r *DirectRepository) ClientOptions() ClientOptions {
	return r.cliOpts
}

// Hostname returns the hostname that connected to the repository.
func (r *DirectRepository) Hostname() string { return r.cliOpts.Hostname }

// Username returns the username that's connect to the repository.
func (r *DirectRepository) Username() string { return r.cliOpts.Username }

// BlobStorage returns the blob storage.
func (r *DirectRepository) BlobStorage() blob.Storage {
	return r.Blobs
}

// ContentManager returns the content manager.
func (r *DirectRepository) ContentManager() *content.WriteManager {
	return r.Content
}

// ConfigFilename returns the name of the configuration file.
func (r *DirectRepository) ConfigFilename() string {
	return r.ConfigFile
}

// OpenObject opens the reader for a given object, returns object.ErrNotFound.
func (r *DirectRepository) OpenObject(ctx context.Context, id object.ID) (object.Reader, error) {
	return object.Open(ctx, r.Content, id)
}

// NewObjectWriter creates an object writer.
func (r *DirectRepository) NewObjectWriter(ctx context.Context, opt object.WriterOptions) object.Writer {
	return r.Objects.NewWriter(ctx, opt)
}

// VerifyObject verifies that the given object is stored properly in a repository and returns backing content IDs.
func (r *DirectRepository) VerifyObject(ctx context.Context, id object.ID) ([]content.ID, error) {
	return object.VerifyObject(ctx, r.Content, id)
}

// GetManifest returns the given manifest data and metadata.
func (r *DirectRepository) GetManifest(ctx context.Context, id manifest.ID, data interface{}) (*manifest.EntryMetadata, error) {
	return r.Manifests.Get(ctx, id, data)
}

// PutManifest saves the given manifest payload with a set of labels.
func (r *DirectRepository) PutManifest(ctx context.Context, labels map[string]string, payload interface{}) (manifest.ID, error) {
	return r.Manifests.Put(ctx, labels, payload)
}

// FindManifests returns metadata for manifests matching given set of labels.
func (r *DirectRepository) FindManifests(ctx context.Context, labels map[string]string) ([]*manifest.EntryMetadata, error) {
	return r.Manifests.Find(ctx, labels)
}

// DeleteManifest deletes the manifest with a given ID.
func (r *DirectRepository) DeleteManifest(ctx context.Context, id manifest.ID) error {
	return r.Manifests.Delete(ctx, id)
}

// ListActiveSessions returns the map of active sessions.
func (r *DirectRepository) ListActiveSessions(ctx context.Context) (map[content.SessionID]*content.SessionInfo, error) {
	return r.Content.ListActiveSessions(ctx)
}

// UpdateDescription updates the description of a connected repository.
func (r *DirectRepository) UpdateDescription(d string) {
	r.cliOpts.Description = d
}

// Close closes the repository and releases all resources.
func (r *DirectRepository) Close(ctx context.Context) error {
	select {
	case <-r.closed:
		// already closed
		return nil

	default:
	}

	if err := r.Flush(ctx); err != nil {
		return errors.Wrap(err, "error flushing")
	}

	if err := r.Objects.Close(); err != nil {
		return errors.Wrap(err, "error closing object manager")
	}

	if err := r.Content.Close(ctx); err != nil {
		return errors.Wrap(err, "error closing content-addressable storage manager")
	}

	if err := r.Blobs.Close(ctx); err != nil {
		return errors.Wrap(err, "error closing blob storage")
	}

	close(r.closed)

	return nil
}

// Flush waits for all in-flight writes to complete.
func (r *DirectRepository) Flush(ctx context.Context) error {
	if err := r.Manifests.Flush(ctx); err != nil {
		return errors.Wrap(err, "error flushing manifests")
	}

	return r.Content.Flush(ctx)
}

// Refresh periodically makes external changes visible to repository.
func (r *DirectRepository) Refresh(ctx context.Context) error {
	updated, err := r.Content.Refresh(ctx)
	if err != nil {
		return errors.Wrap(err, "error refreshing content index")
	}

	if !updated {
		return nil
	}

	log(ctx).Debugf("content index refreshed")

	if err := r.Manifests.Refresh(ctx); err != nil {
		return errors.Wrap(err, "error reloading manifests")
	}

	log(ctx).Debugf("manifests refreshed")

	return nil
}

// RefreshPeriodically periodically refreshes the repository to reflect the changes made by other hosts.
func (r *DirectRepository) RefreshPeriodically(ctx context.Context, interval time.Duration) {
	for {
		select {
		case <-r.closed:
			// stop background refresh when repository is closed
			return

		case <-ctx.Done():
			return

		case <-time.After(interval):
			if err := r.Refresh(ctx); err != nil {
				log(ctx).Warningf("error refreshing repository: %v", err)
			}
		}
	}
}

// Time returns the current local time for the repo.
func (r *DirectRepository) Time() time.Time {
	return defaultTime(r.timeNow)()
}

func defaultTime(f func() time.Time) func() time.Time {
	if f != nil {
		return f
	}

	return clock.Now
}
