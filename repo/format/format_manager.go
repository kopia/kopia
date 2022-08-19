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

// UniqueIDLength is the length of random unique ID of each repository.
const UniqueIDLength = 32

// Manager manages the contents of `kopia.repository` and `kopia.blobcfg`.
type Manager struct {
	//nolint:containedctx
	ctx           context.Context // +checklocksignore
	blobs         blob.Storage    // +checklocksignore
	validDuration time.Duration   // +checklocksignore
	password      string          // +checklocksignore
	cache         BlobCache       // +checklocksignore

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
}

func (m *Manager) getFormat() Provider {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.current
}

func (m *Manager) getOrRefreshFormat() (Provider, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.maybeRefreshLocked(); err != nil {
		return nil, err
	}

	return m.current, nil
}

// +checklocks:m.mu
func (m *Manager) maybeRefreshLocked() error {
	if n := m.timeNow(); !n.Before(m.validUntil) {
		// current format not valid anymore, kick off a refresh
		if err := m.refreshLocked(m.ctx); err != nil {
			return err
		}
	}

	return nil
}

// readAndCacheRepositoryBlobBytes reads the provided blob from the repository or cache directory.
func (m *Manager) readAndCacheRepositoryBlobBytes(ctx context.Context, blobID blob.ID) ([]byte, time.Time, error) {
	if data, mtime, ok := m.cache.Get(ctx, blobID); ok {
		// read from cache and still valid
		age := m.timeNow().Sub(mtime)

		if age < m.validDuration {
			return data, mtime, nil
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

// RefreshCount returns the number of time the format has been refreshed.
func (m *Manager) RefreshCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.refreshCounter
}

// refreshLocked reads `kopia.repository` blob, potentially from cache and decodes it.
// +checklocks:m.mu
func (m *Manager) refreshLocked(ctx context.Context) error {
	log(ctx).Infow("refreshLocked", "now", m.timeNow())

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
		return errors.Errorf("unable to add checksum")
	}

	var formatEncryptionKey []byte

	// try decrypting using old key, if present to avoid deriving it, which is expensive
	repoConfig, err := j.decryptRepositoryConfig(m.formatEncryptionKey)
	if err == nil {
		// still valid, no need to derive
		formatEncryptionKey = m.formatEncryptionKey
	} else {
		formatEncryptionKey, err = j.DeriveFormatEncryptionKeyFromPassword(m.password)
		if err != nil {
			return errors.Wrap(err, "derive format encryption key")
		}

		repoConfig, err = j.decryptRepositoryConfig(formatEncryptionKey)
		if err != nil {
			return ErrInvalidPassword
		}
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

	prov, err := NewStaticProvider(repoConfig, b)
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
	m.refreshCounter++

	return nil
}

// GetEncryptionAlgorithm returns the encryption algorithm.
func (m *Manager) GetEncryptionAlgorithm() string {
	return m.getFormat().GetEncryptionAlgorithm()
}

// GetHashFunction returns the hash function.
func (m *Manager) GetHashFunction() string {
	return m.getFormat().GetHashFunction()
}

// GetECCAlgorithm returns the ECC algorithm.
func (m *Manager) GetECCAlgorithm() string {
	return m.getFormat().GetECCAlgorithm()
}

// GetECCOverheadPercent returns the ECC overhead percent.
func (m *Manager) GetECCOverheadPercent() int {
	return m.getFormat().GetECCOverheadPercent()
}

// GetHmacSecret returns the HMAC function.
func (m *Manager) GetHmacSecret() []byte {
	return m.getFormat().GetHmacSecret()
}

// HashFunc returns the resolved hash function.
func (m *Manager) HashFunc() hashing.HashFunc {
	return m.getFormat().HashFunc()
}

// Encryptor returns the resolved encryptor.
func (m *Manager) Encryptor() encryption.Encryptor {
	return m.getFormat().Encryptor()
}

// GetMasterKey gets the master key.
func (m *Manager) GetMasterKey() []byte {
	return m.getFormat().GetMasterKey()
}

// SupportsPasswordChange returns true if the repository supports password change.
func (m *Manager) SupportsPasswordChange() bool {
	return m.getFormat().SupportsPasswordChange()
}

// RepositoryFormatBytes returns the bytes of `kopia.repository` blob.
// This function blocks to refresh the format blob if necessary.
func (m *Manager) RepositoryFormatBytes() ([]byte, error) {
	f, err := m.getOrRefreshFormat()
	if err != nil {
		return nil, err
	}

	//nolint:wrapcheck
	return f.RepositoryFormatBytes()
}

// GetMutableParameters gets mutable paramers of the repository.
// This function blocks to refresh the format blob if necessary.
func (m *Manager) GetMutableParameters() (MutableParameters, error) {
	f, err := m.getOrRefreshFormat()
	if err != nil {
		return MutableParameters{}, err
	}

	//nolint:wrapcheck
	return f.GetMutableParameters()
}

// UpgradeLockIntent returns the current lock intent.
func (m *Manager) UpgradeLockIntent() (*UpgradeLockIntent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.maybeRefreshLocked(); err != nil {
		return nil, err
	}

	return m.repoConfig.UpgradeLock.Clone(), nil
}

// RequiredFeatures returns the list of features required to open the repository.
func (m *Manager) RequiredFeatures() ([]feature.Required, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.maybeRefreshLocked(); err != nil {
		return nil, err
	}

	return m.repoConfig.RequiredFeatures, nil
}

// LoadedTime gets the time when the config was last reloaded.
func (m *Manager) LoadedTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.loadedTime
}

// updateRepoConfigLocked updates repository config and rewrites kopia.repository blob.
// +checklocks:m.mu
func (m *Manager) updateRepoConfigLocked(ctx context.Context) error {
	if err := m.j.EncryptRepositoryConfig(m.repoConfig, m.formatEncryptionKey); err != nil {
		return errors.Errorf("unable to encrypt format bytes")
	}

	if err := m.j.WriteKopiaRepositoryBlob(ctx, m.blobs, m.blobCfgBlob); err != nil {
		return errors.Wrap(err, "unable to write format blob")
	}

	m.cache.Remove(ctx, []blob.ID{KopiaRepositoryBlobID})

	return nil
}

// UniqueID gets the unique ID of a repository allocated at creation time.
func (m *Manager) UniqueID() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.j.UniqueID
}

// BlobCfgBlob gets the BlobStorageConfiguration.
func (m *Manager) BlobCfgBlob() (BlobStorageConfiguration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.maybeRefreshLocked(); err != nil {
		return BlobStorageConfiguration{}, err
	}

	return m.blobCfgBlob, nil
}

// ObjectFormat gets the object format.
func (m *Manager) ObjectFormat() ObjectFormat {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.repoConfig.ObjectFormat
}

// FormatEncryptionKey gets the format encryption key derived from the password.
func (m *Manager) FormatEncryptionKey() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.formatEncryptionKey
}

// ScrubbedContentFormat returns scrubbed content format with all sensitive data replaced.
func (m *Manager) ScrubbedContentFormat() ContentFormat {
	m.mu.Lock()
	defer m.mu.Unlock()

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
	cache BlobCache,
) (*Manager, error) {
	m := &Manager{
		ctx:           ctx,
		blobs:         st,
		validDuration: validDuration,
		password:      password,
		cache:         cache,
		timeNow:       timeNow,
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	err := m.refreshLocked(ctx)

	return m, err
}

// ErrAlreadyInitialized indicates that repository has already been initialized.
var ErrAlreadyInitialized = errors.Errorf("repository already initialized")

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
		return errors.Errorf("possible corruption: blobcfg blob exists, but format blob is not found")
	}

	if !errors.Is(err, blob.ErrBlobNotFound) {
		return errors.Wrap(err, "unexpected error when checking for blobcfg blob")
	}

	if formatBlob.EncryptionAlgorithm == "" {
		formatBlob.EncryptionAlgorithm = DefaultFormatEncryption
	}

	if formatBlob.KeyDerivationAlgorithm == "" {
		formatBlob.KeyDerivationAlgorithm = DefaultKeyDerivationAlgorithm
	}

	if len(formatBlob.UniqueID) == 0 {
		formatBlob.UniqueID = randomBytes(UniqueIDLength)
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
