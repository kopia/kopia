package repo

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/crypto/scrypt"

	"github.com/kopia/kopia/internal/atomicfile"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/beforeop"
	loggingwrapper "github.com/kopia/kopia/repo/blob/logging"
	"github.com/kopia/kopia/repo/blob/readonly"
	"github.com/kopia/kopia/repo/blob/throttling"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

// CacheDirMarkerFile is the name of the marker file indicating a directory contains Kopia caches.
// See https://bford.info/cachedir/
const CacheDirMarkerFile = "CACHEDIR.TAG"

// CacheDirMarkerHeader is the header signature for cache dir marker files.
const CacheDirMarkerHeader = "Signature: 8a477f597d28d172789f06886806bc55"

// DefaultRepositoryBlobCacheDuration is the duration for which we treat cached kopia.repository
// as valid.
const DefaultRepositoryBlobCacheDuration = 15 * time.Minute

// throttlingWindow is the duration window during which the throttling token bucket fully replenishes.
// the maximum number of tokens in the bucket is multiplied by the number of seconds.
const throttlingWindow = 60 * time.Second

// start with 10% of tokens in the bucket.
const throttleBucketInitialFill = 0.1

// localCacheIntegrityHMACSecretLength length of HMAC secret protecting local cache items.
const localCacheIntegrityHMACSecretLength = 16

// nolint:gochecknoglobals
var localCacheIntegrityPurpose = []byte("local-cache-integrity")

const cacheDirMarkerContents = CacheDirMarkerHeader + `
#
# This file is a cache directory tag created by Kopia - Fast And Secure Open-Source Backup.
#
# For information about Kopia, see:
#   https://kopia.io
#
# For information about cache directory tags, see:
#   http://www.brynosaurus.com/cachedir/
`

var log = logging.Module("kopia/repo")

// Options provides configuration parameters for connection to a repository.
type Options struct {
	TraceStorage        bool             // Logs all storage access using provided Printf-style function
	TimeNowFunc         func() time.Time // Time provider
	DisableInternalLog  bool             // Disable internal log
	UpgradeOwnerID      string           // Owner-ID of any upgrade in progress, when this is not set the access may be restricted
	DoNotWaitForUpgrade bool             // Disable the exponential forever backoff on an upgrade lock.
}

// ErrInvalidPassword is returned when repository password is invalid.
var ErrInvalidPassword = errors.Errorf("invalid repository password")

// ErrRepositoryUnavailableDueToUpgrageInProgress is returned when repository
// is undergoing upgrade that requires exclusive access.
var ErrRepositoryUnavailableDueToUpgrageInProgress = errors.Errorf("repository upgrade in progress")

// Open opens a Repository specified in the configuration file.
func Open(ctx context.Context, configFile, password string, options *Options) (rep Repository, err error) {
	ctx, span := tracer.Start(ctx, "OpenRepository")
	defer span.End()

	defer func() {
		if err != nil {
			log(ctx).Errorf("failed to open repository: %v", err)
		}
	}()

	if options == nil {
		options = &Options{}
	}

	configFile, err = filepath.Abs(configFile)
	if err != nil {
		return nil, errors.Wrap(err, "error resolving config file path")
	}

	lc, err := LoadConfigFromFile(configFile)
	if err != nil {
		return nil, err
	}

	if lc.APIServer != nil {
		return OpenAPIServer(ctx, lc.APIServer, lc.ClientOptions, lc.Caching, password)
	}

	return openDirect(ctx, configFile, lc, password, options)
}

func getContentCacheOrNil(ctx context.Context, opt *content.CachingOptions, password string) (*cache.PersistentCache, error) {
	opt = opt.CloneOrDefault()

	cs, err := cache.NewStorageOrNil(ctx, opt.CacheDirectory, opt.MaxCacheSizeBytes, "server-contents")
	if cs == nil {
		// this may be (nil, nil) or (nil, err)
		return nil, errors.Wrap(err, "error opening storage")
	}

	// derive content cache key from the password & HMAC secret using scrypt.
	salt := append([]byte("content-cache-protection"), opt.HMACSecret...)

	// nolint:gomnd
	cacheEncryptionKey, err := scrypt.Key([]byte(password), salt, 65536, 8, 1, 32)
	if err != nil {
		return nil, errors.Wrap(err, "unable to derive cache encryption key from password")
	}

	prot, err := cache.AuthenticatedEncryptionProtection(cacheEncryptionKey)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize protection")
	}

	pc, err := cache.NewPersistentCache(ctx, "cache-storage", cs, prot, cache.SweepSettings{
		MaxSizeBytes: opt.MaxCacheSizeBytes,
		MinSweepAge:  opt.MinContentSweepAge.DurationOrDefault(content.DefaultDataCacheSweepAge),
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to open persistent cache")
	}

	return pc, nil
}

// OpenAPIServer connects remote repository over Kopia API.
func OpenAPIServer(ctx context.Context, si *APIServerInfo, cliOpts ClientOptions, cachingOptions *content.CachingOptions, password string) (Repository, error) {
	contentCache, err := getContentCacheOrNil(ctx, cachingOptions, password)
	if err != nil {
		return nil, errors.Wrap(err, "error opening content cache")
	}

	if si.DisableGRPC {
		return openRestAPIRepository(ctx, si, cliOpts, contentCache, password)
	}

	return OpenGRPCAPIRepository(ctx, si, cliOpts, contentCache, password)
}

// openDirect opens the repository that directly manipulates blob storage..
func openDirect(ctx context.Context, configFile string, lc *LocalConfig, password string, options *Options) (rep Repository, err error) {
	if lc.Storage == nil {
		return nil, errors.Errorf("storage not set in the configuration file")
	}

	st, err := blob.NewStorage(ctx, *lc.Storage, false)
	if err != nil {
		return nil, errors.Wrap(err, "cannot open storage")
	}

	if options.TraceStorage {
		st = loggingwrapper.NewWrapper(st, log(ctx), "[STORAGE] ")
	}

	if lc.ReadOnly {
		st = readonly.NewWrapper(st)
	}

	r, err := openWithConfig(ctx, st, lc, password, options, lc.Caching, configFile)
	if err != nil {
		st.Close(ctx) //nolint:errcheck
		return nil, err
	}

	return r, nil
}

type unpackedFormatBlob struct {
	f                   *formatBlob
	fb                  []byte                  // serialized format blob
	cacheMTime          time.Time               // mod time of the format blob cache file
	repoConfig          *repositoryObjectFormat // unencrypted format blob structure
	formatEncryptionKey []byte                  // key derived from the password
}

func readAndCacheRepoConfig(ctx context.Context, st blob.Storage, password string, cacheOpts *content.CachingOptions, validDuration time.Duration) (ufb *unpackedFormatBlob, err error) {
	ufb = &unpackedFormatBlob{}

	// Read format blob, potentially from cache.
	ufb.fb, ufb.cacheMTime, err = readAndCacheRepositoryBlobBytes(ctx, st, cacheOpts.CacheDirectory, FormatBlobID, validDuration)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read format blob")
	}

	if err = writeCacheMarker(cacheOpts.CacheDirectory); err != nil {
		return nil, errors.Wrap(err, "unable to write cache directory marker")
	}

	ufb.f, err = parseFormatBlob(ufb.fb)
	if err != nil {
		return nil, errors.Wrap(err, "can't parse format blob")
	}

	ufb.fb, err = addFormatBlobChecksumAndLength(ufb.fb)
	if err != nil {
		return nil, errors.Errorf("unable to add checksum")
	}

	ufb.formatEncryptionKey, err = ufb.f.deriveFormatEncryptionKeyFromPassword(password)
	if err != nil {
		return nil, err
	}

	ufb.repoConfig, err = ufb.f.decryptFormatBytes(ufb.formatEncryptionKey)
	if err != nil {
		return nil, ErrInvalidPassword
	}

	return ufb, nil
}

// ReadAndCacheRepoUpgradeLock loads the lock config from cache and returns it.
func ReadAndCacheRepoUpgradeLock(ctx context.Context, st blob.Storage, password string, cacheOpts *content.CachingOptions, validDuration time.Duration) (*content.UpgradeLock, error) {
	ufb, err := readAndCacheRepoConfig(ctx, st, password, cacheOpts, validDuration)
	return ufb.repoConfig.FormattingOptions.UpgradeLock, err
}

// openWithConfig opens the repository with a given configuration, avoiding the need for a config file.
// nolint:funlen,gocyclo
func openWithConfig(ctx context.Context, st blob.Storage, lc *LocalConfig, password string, options *Options, cacheOpts *content.CachingOptions, configFile string) (DirectRepository, error) {
	cacheOpts = cacheOpts.CloneOrDefault()
	cmOpts := &content.ManagerOptions{
		TimeNow:            defaultTime(options.TimeNowFunc),
		DisableInternalLog: options.DisableInternalLog,
	}

	var ufb *unpackedFormatBlob

	if _, err := retry.WithExponentialBackoffMaxRetries(ctx, -1, "read repo config and wait for upgrade", func() (interface{}, error) {
		var internalErr error
		ufb, internalErr = readAndCacheRepoConfig(ctx, st, password, cacheOpts,
			lc.FormatBlobCacheDuration)
		if internalErr != nil {
			return nil, internalErr
		}

		// retry if upgrade lock has been taken
		if locked, _ := ufb.repoConfig.UpgradeLock.IsLocked(cmOpts.TimeNow()); locked && options.UpgradeOwnerID != ufb.repoConfig.UpgradeLock.OwnerID {
			return nil, ErrRepositoryUnavailableDueToUpgrageInProgress
		}

		return nil, nil
	}, func(internalErr error) bool {
		return !options.DoNotWaitForUpgrade && errors.Is(internalErr, ErrRepositoryUnavailableDueToUpgrageInProgress)
	}); err != nil {
		// nolint:wrapcheck
		return nil, err
	}

	cmOpts.RepositoryFormatBytes = ufb.fb

	// Read blobcfg blob, potentially from cache.
	bb, _, err := readAndCacheRepositoryBlobBytes(ctx, st, cacheOpts.CacheDirectory, BlobCfgBlobID, lc.FormatBlobCacheDuration)
	if err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
		return nil, errors.Wrap(err, "unable to read blobcfg blob")
	}

	blobcfg, err := deserializeBlobCfgBytes(ufb.f, bb, ufb.formatEncryptionKey)
	if err != nil {
		return nil, ErrInvalidPassword
	}

	if ufb.repoConfig.FormattingOptions.EnablePasswordChange {
		cacheOpts.HMACSecret = deriveKeyFromMasterKey(ufb.repoConfig.HMACSecret, ufb.f.UniqueID, localCacheIntegrityPurpose, localCacheIntegrityHMACSecretLength)
	} else {
		// deriving from ufb.formatEncryptionKey was actually a bug, that only matters will change when we change the password
		cacheOpts.HMACSecret = deriveKeyFromMasterKey(ufb.formatEncryptionKey, ufb.f.UniqueID, localCacheIntegrityPurpose, localCacheIntegrityHMACSecretLength)
	}

	fo := &ufb.repoConfig.FormattingOptions

	if fo.MaxPackSize == 0 {
		// legacy only, apply default
		fo.MaxPackSize = 20 << 20 // nolint:gomnd
	}

	// do not embed repository format info in pack blobs when password change is enabled.
	if fo.EnablePasswordChange {
		cmOpts.RepositoryFormatBytes = nil
	}

	limits := throttlingLimitsFromConnectionInfo(ctx, st.ConnectionInfo())
	if lc.Throttling != nil {
		limits = *lc.Throttling
	}

	st, throttler, err := addThrottler(st, limits)
	if err != nil {
		return nil, errors.Wrap(err, "unable to add throttler")
	}

	throttler.OnUpdate(func(l throttling.Limits) error {
		lc2, err2 := LoadConfigFromFile(configFile)
		if err2 != nil {
			return err2
		}

		lc2.Throttling = &l

		return lc2.writeToFile(configFile)
	})

	if blobcfg.IsRetentionEnabled() {
		st = wrapLockingStorage(st, blobcfg)
	}

	// background/interleaving upgrade lock storage monitor
	st = upgradeLockMonitor(options.UpgradeOwnerID, st, password, cacheOpts, lc.FormatBlobCacheDuration,
		ufb.cacheMTime, cmOpts.TimeNow)

	scm, err := content.NewSharedManager(ctx, st, fo, cacheOpts, cmOpts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create shared content manager")
	}

	cm := content.NewWriteManager(ctx, scm, content.SessionOptions{
		SessionUser: lc.Username,
		SessionHost: lc.Hostname,
	}, "")

	om, err := object.NewObjectManager(ctx, cm, ufb.repoConfig.Format)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open object manager")
	}

	manifests, err := manifest.NewManager(ctx, cm, manifest.ManagerOptions{TimeNow: cmOpts.TimeNow})
	if err != nil {
		return nil, errors.Wrap(err, "unable to open manifests")
	}

	dr := &directRepository{
		cmgr:  cm,
		omgr:  om,
		blobs: st,
		mmgr:  manifests,
		sm:    scm,
		directRepositoryParameters: directRepositoryParameters{
			uniqueID:            ufb.f.UniqueID,
			cachingOptions:      *cacheOpts,
			formatBlob:          ufb.f,
			blobCfgBlob:         blobcfg,
			formatEncryptionKey: ufb.formatEncryptionKey,
			timeNow:             cmOpts.TimeNow,
			cliOpts:             lc.ClientOptions.ApplyDefaults(ctx, "Repository in "+st.DisplayName()),
			configFile:          configFile,
			nextWriterID:        new(int32),
			throttler:           throttler,
		},
		closed: make(chan struct{}),
	}

	return dr, nil
}

func wrapLockingStorage(st blob.Storage, r content.BlobCfgBlob) blob.Storage {
	// collect prefixes that need to be locked on put
	var prefixes []string
	for _, prefix := range content.PackBlobIDPrefixes {
		prefixes = append(prefixes, string(prefix))
	}

	prefixes = append(prefixes, content.IndexBlobPrefix, epoch.EpochManagerIndexUberPrefix, FormatBlobID,
		BlobCfgBlobID)

	return beforeop.NewWrapper(st, nil, nil, nil, func(ctx context.Context, id blob.ID, opts *blob.PutOptions) error {
		for _, prefix := range prefixes {
			if strings.HasPrefix(string(id), prefix) {
				opts.RetentionMode = r.RetentionMode
				opts.RetentionPeriod = r.RetentionPeriod
				break
			}
		}
		return nil
	})
}

func addThrottler(st blob.Storage, limits throttling.Limits) (blob.Storage, throttling.SettableThrottler, error) {
	throttler, err := throttling.NewThrottler(limits, throttlingWindow, throttleBucketInitialFill)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create throttler")
	}

	return throttling.NewWrapper(st, throttler), throttler, nil
}

func upgradeLockMonitor(
	upgradeOwnerID string,
	st blob.Storage,
	password string,
	cacheOpts *content.CachingOptions,
	lockRefreshInterval time.Duration,
	lastSync time.Time,
	now func() time.Time,
) blob.Storage {
	var (
		m        sync.RWMutex
		nextSync = lastSync.Add(lockRefreshInterval)
	)

	cb := func(ctx context.Context) error {
		// protected read for nextSync because it will be shared between
		// parallel storage operations
		m.RLock()
		if nextSync.After(now()) {
			m.RUnlock()
			return nil
		}
		m.RUnlock()

		// upgrade the lock and verify again in-case someone else won the race to refresh
		m.Lock()
		defer m.Unlock()

		if nextSync.After(now()) {
			return nil
		}

		ufb, err := readAndCacheRepoConfig(ctx, st, password, cacheOpts, lockRefreshInterval)
		if err != nil {
			return err
		}

		// only allow the upgrade owner to perform storage operations
		if locked, _ := ufb.repoConfig.UpgradeLock.IsLocked(now()); locked && upgradeOwnerID != ufb.repoConfig.UpgradeLock.OwnerID {
			return ErrRepositoryUnavailableDueToUpgrageInProgress
		}

		// prevent backward jumps on nextSync
		newNextSync := ufb.cacheMTime.Add(lockRefreshInterval)
		if newNextSync.After(nextSync) {
			nextSync = newNextSync
		}

		return nil
	}

	return beforeop.NewUniformWrapper(st, cb)
}

func throttlingLimitsFromConnectionInfo(ctx context.Context, ci blob.ConnectionInfo) throttling.Limits {
	v, err := json.Marshal(ci.Config)
	if err != nil {
		return throttling.Limits{}
	}

	var l throttling.Limits

	if err := json.Unmarshal(v, &l); err != nil {
		return throttling.Limits{}
	}

	log(ctx).Debugw("throttling limits from connection info", "limits", l)

	return l
}

func writeCacheMarker(cacheDir string) error {
	if cacheDir == "" {
		return nil
	}

	markerFile := filepath.Join(cacheDir, CacheDirMarkerFile)

	st, err := os.Stat(markerFile)
	if err == nil && st.Size() >= int64(len(cacheDirMarkerContents)) {
		// ok
		return nil
	}

	if err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "unexpected cache marker error")
	}

	f, err := os.Create(markerFile) //nolint:gosec
	if err != nil {
		return errors.Wrap(err, "error creating cache marker")
	}

	if _, err := f.WriteString(cacheDirMarkerContents); err != nil {
		return errors.Wrap(err, "unable to write cachedir marker contents")
	}

	return errors.Wrap(f.Close(), "error closing cache marker file")
}

func formatBytesCachingEnabled(cacheDirectory string, validDuration time.Duration) bool {
	if cacheDirectory == "" {
		return false
	}

	return validDuration > 0
}

func readRepositoryBlobBytesFromCache(ctx context.Context, cachedFile string, validDuration time.Duration) (data []byte, cacheMTime time.Time, err error) {
	cst, err := os.Stat(cachedFile)
	if err != nil {
		return nil, time.Time{}, errors.Wrap(err, "unable to open cache file")
	}

	cacheMTime = cst.ModTime()
	if clock.Now().Sub(cacheMTime) > validDuration {
		// got cached file, but it's too old, remove it
		if err = os.Remove(cachedFile); err != nil {
			log(ctx).Debugf("unable to remove cache file: %v", err)
		}

		return nil, time.Time{}, errors.Errorf("cached file too old")
	}

	data, err = os.ReadFile(cachedFile) // nolint:gosec
	if err != nil {
		return nil, time.Time{}, errors.Wrapf(err, "failed to read the cache file %q", cachedFile)
	}

	return data, cacheMTime, nil
}

func readAndCacheRepositoryBlobBytes(ctx context.Context, st blob.Storage, cacheDirectory, blobID string, validDuration time.Duration) ([]byte, time.Time, error) {
	cachedFile := filepath.Join(cacheDirectory, blobID)

	if validDuration == 0 {
		validDuration = DefaultRepositoryBlobCacheDuration
	}

	if cacheDirectory != "" {
		if err := os.MkdirAll(cacheDirectory, cache.DirMode); err != nil && !os.IsExist(err) {
			log(ctx).Errorf("unable to create cache directory: %v", err)
		}
	}

	cacheEnabled := formatBytesCachingEnabled(cacheDirectory, validDuration)
	if cacheEnabled {
		data, cacheMTime, err := readRepositoryBlobBytesFromCache(ctx, cachedFile, validDuration)
		if err == nil {
			log(ctx).Debugf("%s retrieved from cache", blobID)

			return data, cacheMTime, nil
		}

		if os.IsNotExist(err) {
			log(ctx).Debugf("%s could not be fetched from cache: %v", blobID, err)
		}
	} else {
		log(ctx).Debugf("%s cache not enabled", blobID)
	}

	var b gather.WriteBuffer
	defer b.Close()

	if err := st.GetBlob(ctx, blob.ID(blobID), 0, -1, &b); err != nil {
		return nil, time.Time{}, errors.Wrapf(err, "error getting %s blob", blobID)
	}

	if cacheEnabled {
		if err := atomicfile.Write(cachedFile, b.Bytes().Reader()); err != nil {
			log(ctx).Warnf("unable to write cache: %v", err)
		}
	}

	return b.ToByteSlice(), clock.Now(), nil
}
