package format

import (
	"context"
	"crypto/rand"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/feature"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("kopia/repo/format")

// UniqueIDLengthBytes is the length of random unique ID of each repository.
const UniqueIDLengthBytes = 32

// Manager manages the contents of `kopia.repository` and `kopia.blobcfg`.
type Manager struct {
	blobs         blob.Storage  // +checklocksignore
	validDuration time.Duration // +checklocksignore
	password      string        // +checklocksignore
	cache         blobCache     // +checklocksignore

	// provider for immutable parts of the format data, used to avoid locks.
	immutable Provider

	timeNow func() time.Time // +checklocksignore

	// all the stuff protected by a mutex is valid until `validUntil`
	mu sync.RWMutex
	// +checklocks:mu
	formatEncryptionKey []byte
	// +checklocks:mu
	j *KopiaRepositoryJSON
	// +checklocks:mu
	repoConfig *RepositoryConfig
	// +checklocks:mu
	blobCfgBlob BlobStorageConfiguration
	// +checklocks:mu
	current Provider
	// +checklocks:mu
	validUntil time.Time
	// +checklocks:mu
	loadedTime time.Time
	// +checklocks:mu
	refreshCounter int
	// +checklocks:mu
	ignoreCacheOnFirstRefresh bool
}

func (m *Manager) getOrRefreshFormat(ctx context.Context) (Provider, error) {
	if err := m.maybeRefreshNotLocked(ctx); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.current, nil
}

func (m *Manager) maybeRefreshNotLocked(ctx context.Context) error {
	m.mu.RLock()
	val := m.validUntil
	m.mu.RUnlock()

	if m.timeNow().Before(val) {
		return nil
	}

	// current format not valid anymore, kick off a refresh
	return m.refresh(ctx)
}

// readAndCacheRepositoryBlobBytes reads the provided blob from the repository or cache directory.
// +checklocks:m.mu
func (m *Manager) readAndCacheRepositoryBlobBytes(ctx context.Context, blobID blob.ID) ([]byte, time.Time, error) {
	if !m.ignoreCacheOnFirstRefresh {
		if data, mtime, ok := m.cache.Get(ctx, blobID); ok {
			// read from cache and still valid
			age := m.timeNow().Sub(mtime)

			if age < m.validDuration {
				return data, mtime, nil
			}
		}
	}

	var b gather.WriteBuffer
	defer b.Close()

	if err := m.blobs.GetBlob(ctx, blobID, 0, -1, &b); err != nil {
		return nil, time.Time{}, errors.Wrapf(err, "error getting %s blob", blobID)
	}

	data := b.ToByteSlice()

	mtime, err := m.cache.Put(ctx, blobID, data)

	return data, mtime, errors.Wrapf(err, "error adding %s blob", blobID)
}

// ValidCacheDuration returns the duration for which each blob in the cache is valid.
func (m *Manager) ValidCacheDuration() time.Duration {
	return m.validDuration
}

// RefreshCount returns the number of time the format has been refreshed.
func (m *Manager) RefreshCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.refreshCounter
}

// refresh reads `kopia.repository` blob, potentially from cache and decodes it.
func (m *Manager) refresh(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, cacheMTime, err := m.readAndCacheRepositoryBlobBytes(ctx, KopiaRepositoryBlobID)
	if err != nil {
		return errors.Wrap(err, "unable to read format blob")
	}

	j, err := ParseKopiaRepositoryJSON(b)
	if err != nil {
		return errors.Wrap(err, "can't parse format blob")
	}

	b, err = addFormatBlobChecksumAndLength(b)
	if err != nil {
		return errors.New("unable to add checksum")
	}

	// use old key, if present to avoid deriving it, which is expensive
	formatEncryptionKey := m.formatEncryptionKey
	if len(m.formatEncryptionKey) == 0 {
		formatEncryptionKey, err = j.DeriveFormatEncryptionKeyFromPassword(m.password)
		if err != nil {
			return errors.Wrap(err, "derive format encryption key")
		}
	}

	repoConfig, err := j.decryptRepositoryConfig(formatEncryptionKey)
	if err != nil {
		return ErrInvalidPassword
	}

	var blobCfg BlobStorageConfiguration

	if b2, _, err2 := m.readAndCacheRepositoryBlobBytes(ctx, KopiaBlobCfgBlobID); err2 == nil {
		var e2 error

		blobCfg, e2 = deserializeBlobCfgBytes(j, b2, formatEncryptionKey)
		if e2 != nil {
			return errors.Wrap(e2, "deserialize blob config")
		}
	} else if !errors.Is(err2, blob.ErrBlobNotFound) {
		return errors.Wrap(err2, "load blob config")
	}

	prov, err := NewFormattingOptionsProvider(&repoConfig.ContentFormat, b)
	if err != nil {
		return errors.Wrap(err, "error creating format provider")
	}

	m.current = prov
	m.j = j
	m.repoConfig = repoConfig
	m.validUntil = cacheMTime.Add(m.validDuration)
	m.formatEncryptionKey = formatEncryptionKey
	m.loadedTime = cacheMTime
	m.blobCfgBlob = blobCfg
	m.ignoreCacheOnFirstRefresh = false

	if m.immutable == nil {
		// on first refresh, set `immutable``
		m.immutable = prov
	}

	m.refreshCounter++

	return nil
}

// GetEncryptionAlgorithm returns the encryption algorithm.
func (m *Manager) GetEncryptionAlgorithm() string {
	return m.immutable.GetEncryptionAlgorithm()
}

// GetHashFunction returns the hash function.
func (m *Manager) GetHashFunction() string {
	return m.immutable.GetHashFunction()
}

// GetECCAlgorithm returns the ECC algorithm.
func (m *Manager) GetECCAlgorithm() string {
	return m.immutable.GetECCAlgorithm()
}

// GetECCOverheadPercent returns the ECC overhead percent.
func (m *Manager) GetECCOverheadPercent() int {
	return m.immutable.GetECCOverheadPercent()
}

// GetHmacSecret returns the HMAC function.
func (m *Manager) GetHmacSecret() []byte {
	return m.immutable.GetHmacSecret()
}

// HashFunc returns the resolved hash function.
func (m *Manager) HashFunc() hashing.HashFunc {
	return m.immutable.HashFunc()
}

// Encryptor returns the resolved encryptor.
func (m *Manager) Encryptor() encryption.Encryptor {
	return m.immutable.Encryptor()
}

// GetMasterKey gets the master key.
func (m *Manager) GetMasterKey() []byte {
	return m.immutable.GetMasterKey()
}

// SupportsPasswordChange returns true if the repository supports password change.
func (m *Manager) SupportsPasswordChange() bool {
	return m.immutable.SupportsPasswordChange()
}

// RepositoryFormatBytes returns the bytes of `kopia.repository` blob.
// This function blocks to refresh the format blob if necessary.
func (m *Manager) RepositoryFormatBytes(ctx context.Context) ([]byte, error) {
	f, err := m.getOrRefreshFormat(ctx)
	if err != nil {
		return nil, err
	}

	//nolint:wrapcheck
	return f.RepositoryFormatBytes(ctx)
}

// GetMutableParameters gets mutable paramers of the repository.
// This function blocks to refresh the format blob if necessary.
func (m *Manager) GetMutableParameters(ctx context.Context) (MutableParameters, error) {
	f, err := m.getOrRefreshFormat(ctx)
	if err != nil {
		return MutableParameters{}, err
	}

	//nolint:wrapcheck
	return f.GetMutableParameters(ctx)
}

// GetCachedMutableParameters gets mutable paramers of the repository without blocking.
func (m *Manager) GetCachedMutableParameters() MutableParameters {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.current == nil {
		return MutableParameters{}
	}

	return m.current.GetCachedMutableParameters()
}

// UpgradeLockIntent returns the current lock intent.
func (m *Manager) UpgradeLockIntent(ctx context.Context) (*UpgradeLockIntent, error) {
	if err := m.maybeRefreshNotLocked(ctx); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.repoConfig.UpgradeLock.Clone(), nil
}

// RequiredFeatures returns the list of features required to open the repository.
func (m *Manager) RequiredFeatures(ctx context.Context) ([]feature.Required, error) {
	if err := m.maybeRefreshNotLocked(ctx); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.repoConfig.RequiredFeatures, nil
}

// LoadedTime gets the time when the config was last reloaded.
func (m *Manager) LoadedTime() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.loadedTime
}

// updateRepoConfigLocked updates repository config and rewrites kopia.repository blob.
// +checklocks:m.mu
func (m *Manager) updateRepoConfigLocked(ctx context.Context) error {
	if err := m.j.EncryptRepositoryConfig(m.repoConfig, m.formatEncryptionKey); err != nil {
		return errors.New("unable to encrypt format bytes")
	}

	if err := m.j.WriteKopiaRepositoryBlob(ctx, m.blobs, m.blobCfgBlob); err != nil {
		return errors.Wrap(err, "unable to write format blob")
	}

	m.cache.Remove(ctx, []blob.ID{KopiaRepositoryBlobID})

	return nil
}

// UniqueID gets the unique ID of a repository allocated at creation time.
func (m *Manager) UniqueID() []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.j.UniqueID
}

// BlobCfgBlob gets the BlobStorageConfiguration.
func (m *Manager) BlobCfgBlob(ctx context.Context) (BlobStorageConfiguration, error) {
	if err := m.maybeRefreshNotLocked(ctx); err != nil {
		return BlobStorageConfiguration{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.blobCfgBlob, nil
}

// ObjectFormat gets the object format.
func (m *Manager) ObjectFormat() ObjectFormat {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.repoConfig.ObjectFormat
}

// FormatEncryptionKey gets the format encryption key derived from the password.
func (m *Manager) FormatEncryptionKey() []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.formatEncryptionKey
}

// ScrubbedContentFormat returns scrubbed content format with all sensitive data replaced.
func (m *Manager) ScrubbedContentFormat() ContentFormat {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cf := m.repoConfig.ContentFormat
	cf.MasterKey = nil
	cf.HMACSecret = nil

	return cf
}

// NewManager creates new format manager which automatically refreshes format blob on reads (in a blocking manner).
func NewManager(
	ctx context.Context,
	st blob.Storage,
	cacheDir string,
	validDuration time.Duration,
	password string,
	timeNow func() time.Time,
) (*Manager, error) {
	return NewManagerWithCache(ctx, st, validDuration, password, timeNow, NewFormatBlobCache(cacheDir, validDuration, timeNow))
}

// NewManagerWithCache creates new format manager which automatically refreshes format blob on reads (in a blocking manner)
// and uses the provided cache.
func NewManagerWithCache(
	ctx context.Context,
	st blob.Storage,
	validDuration time.Duration,
	password string,
	timeNow func() time.Time,
	cache blobCache,
) (*Manager, error) {
	var ignoreCacheOnFirstRefresh bool

	if validDuration < 0 {
		// valid duration less than zero indicates we want to skip cache on first access
		validDuration = DefaultRepositoryBlobCacheDuration
		ignoreCacheOnFirstRefresh = true
	}

	if validDuration > DefaultRepositoryBlobCacheDuration {
		log(ctx).Infof("repository format cache duration capped at %v", DefaultRepositoryBlobCacheDuration)

		validDuration = DefaultRepositoryBlobCacheDuration
	}

	m := &Manager{
		blobs:                     st,
		validDuration:             validDuration,
		password:                  password,
		cache:                     cache,
		timeNow:                   timeNow,
		ignoreCacheOnFirstRefresh: ignoreCacheOnFirstRefresh,
	}

	err := m.refresh(ctx)

	return m, err
}

// ErrAlreadyInitialized indicates that repository has already been initialized.
var ErrAlreadyInitialized = errors.New("repository already initialized")

// Initialize initializes the format blob in a given storage.
func Initialize(ctx context.Context, st blob.Storage, formatBlob *KopiaRepositoryJSON, repoConfig *RepositoryConfig, blobcfg BlobStorageConfiguration, password string) error {
	// get the blob - expect ErrNotFound
	var tmp gather.WriteBuffer
	defer tmp.Close()

	err := st.GetBlob(ctx, KopiaRepositoryBlobID, 0, -1, &tmp)
	if err == nil {
		return ErrAlreadyInitialized
	}

	if !errors.Is(err, blob.ErrBlobNotFound) {
		return errors.Wrap(err, "unexpected error when checking for format blob")
	}

	err = st.GetBlob(ctx, KopiaBlobCfgBlobID, 0, -1, &tmp)
	if err == nil {
		return errors.New("possible corruption: blobcfg blob exists, but format blob is not found")
	}

	if !errors.Is(err, blob.ErrBlobNotFound) {
		return errors.Wrap(err, "unexpected error when checking for blobcfg blob")
	}

	if formatBlob.EncryptionAlgorithm == "" {
		formatBlob.EncryptionAlgorithm = DefaultFormatEncryption
	}

	// In legacy versions, the KeyDerivationAlgorithm may not be present in the
	// KopiaRepositoryJson. In those cases default to using Scrypt.
	if formatBlob.KeyDerivationAlgorithm == "" {
		formatBlob.KeyDerivationAlgorithm = DefaultKeyDerivationAlgorithm
	}

	if len(formatBlob.UniqueID) == 0 {
		formatBlob.UniqueID = randomBytes(UniqueIDLengthBytes)
	}

	formatEncryptionKey, err := formatBlob.DeriveFormatEncryptionKeyFromPassword(password)
	if err != nil {
		return errors.Wrap(err, "unable to derive format encryption key")
	}

	if err = repoConfig.MutableParameters.Validate(); err != nil {
		return errors.Wrap(err, "invalid parameters")
	}

	if err = blobcfg.Validate(); err != nil {
		return errors.Wrap(err, "blob config")
	}

	if err = formatBlob.EncryptRepositoryConfig(repoConfig, formatEncryptionKey); err != nil {
		return errors.Wrap(err, "unable to encrypt format bytes")
	}

	if err := formatBlob.WriteBlobCfgBlob(ctx, st, blobcfg, formatEncryptionKey); err != nil {
		return errors.Wrap(err, "unable to write blobcfg blob")
	}

	if err := formatBlob.WriteKopiaRepositoryBlob(ctx, st, blobcfg); err != nil {
		return errors.Wrap(err, "unable to write format blob")
	}

	return nil
}

var _ Provider = (*Manager)(nil)

func randomBytes(n int) []byte {
	b := make([]byte, n)
	io.ReadFull(rand.Reader, b) //nolint:errcheck

	return b
}
