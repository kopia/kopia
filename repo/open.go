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

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/feature"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/beforeop"
	loggingwrapper "github.com/kopia/kopia/repo/blob/logging"
	"github.com/kopia/kopia/repo/blob/readonly"
	"github.com/kopia/kopia/repo/blob/throttling"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

// The list below keeps track of features this version of Kopia supports for forwards compatibility.
//
// Repository can specify which features are required to open it and clients will refuse to open the
// repository if they don't have all required features.
//
// In the future we'll be removing features from the list to deprecate them and this will ensure newer
// versions of kopia won't be able to work with old, unmigrated repositories.
//
// The strings are arbitrary, but should be short, human-readable and immutable once a version
// that starts requiring them is released.
// nolint:gochecknoglobals
var supportedFeatures = []feature.Feature{
	"index-v1",
	"index-v2",
}

// throttlingWindow is the duration window during which the throttling token bucket fully replenishes.
// the maximum number of tokens in the bucket is multiplied by the number of seconds.
const throttlingWindow = 60 * time.Second

// start with 10% of tokens in the bucket.
const throttleBucketInitialFill = 0.1

// localCacheIntegrityHMACSecretLength length of HMAC secret protecting local cache items.
const localCacheIntegrityHMACSecretLength = 16

// nolint:gochecknoglobals
var localCacheIntegrityPurpose = []byte("local-cache-integrity")

var log = logging.Module("kopia/repo")

// Options provides configuration parameters for connection to a repository.
type Options struct {
	TraceStorage        bool             // Logs all storage access using provided Printf-style function
	TimeNowFunc         func() time.Time // Time provider
	DisableInternalLog  bool             // Disable internal log
	UpgradeOwnerID      string           // Owner-ID of any upgrade in progress, when this is not set the access may be restricted
	DoNotWaitForUpgrade bool             // Disable the exponential forever backoff on an upgrade lock.

	OnFatalError func(err error) // function to invoke when repository encounters a fatal error, usually invokes os.Exit

	// test-only flags
	TestOnlyIgnoreMissingRequiredFeatures bool // ignore missing features
}

// ErrInvalidPassword is returned when repository password is invalid.
var ErrInvalidPassword = format.ErrInvalidPassword

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

	if options.OnFatalError == nil {
		options.OnFatalError = func(err error) {
			log(ctx).Errorf("FATAL: %v", err)
			os.Exit(1)
		}
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

// openWithConfig opens the repository with a given configuration, avoiding the need for a config file.
// nolint:funlen,gocyclo,cyclop
func openWithConfig(ctx context.Context, st blob.Storage, lc *LocalConfig, password string, options *Options, cacheOpts *content.CachingOptions, configFile string) (DirectRepository, error) {
	cacheOpts = cacheOpts.CloneOrDefault()
	cmOpts := &content.ManagerOptions{
		TimeNow:            defaultTime(options.TimeNowFunc),
		DisableInternalLog: options.DisableInternalLog,
	}

	var ufb *format.DecodedRepositoryConfig

	if _, err := retry.WithExponentialBackoffMaxRetries(ctx, -1, "read repo config and wait for upgrade", func() (interface{}, error) {
		var internalErr error
		ufb, internalErr = format.ReadAndCacheDecodedRepositoryConfig(ctx, st, password, cacheOpts.CacheDirectory,
			lc.FormatBlobCacheDuration)
		if internalErr != nil {
			// nolint:wrapcheck
			return nil, internalErr
		}

		// retry if upgrade lock has been taken
		if locked, _ := ufb.RepoConfig.UpgradeLock.IsLocked(cmOpts.TimeNow()); locked && options.UpgradeOwnerID != ufb.RepoConfig.UpgradeLock.OwnerID {
			return nil, ErrRepositoryUnavailableDueToUpgrageInProgress
		}

		return nil, nil
	}, func(internalErr error) bool {
		return !options.DoNotWaitForUpgrade && errors.Is(internalErr, ErrRepositoryUnavailableDueToUpgrageInProgress)
	}); err != nil {
		// nolint:wrapcheck
		return nil, err
	}

	if err := handleMissingRequiredFeatures(ctx, ufb.RepoConfig, options.TestOnlyIgnoreMissingRequiredFeatures); err != nil {
		return nil, err
	}

	cmOpts.RepositoryFormatBytes = ufb.KopiaRepositoryBytes

	// Read blobcfg blob, potentially from cache.
	bb, _, err := format.ReadAndCacheRepositoryBlobBytes(ctx, st, cacheOpts.CacheDirectory, format.KopiaBlobCfgBlobID, lc.FormatBlobCacheDuration)
	if err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
		return nil, errors.Wrap(err, "unable to read blobcfg blob")
	}

	blobcfg, err := ufb.KopiaRepository.DeserializeBlobCfgBytes(bb, ufb.FormatEncryptionKey)
	if err != nil {
		return nil, ErrInvalidPassword
	}

	if ufb.RepoConfig.ContentFormat.EnablePasswordChange {
		cacheOpts.HMACSecret = format.DeriveKeyFromMasterKey(ufb.RepoConfig.HMACSecret, ufb.KopiaRepository.UniqueID, localCacheIntegrityPurpose, localCacheIntegrityHMACSecretLength)
	} else {
		// deriving from ufb.FormatEncryptionKey was actually a bug, that only matters will change when we change the password
		cacheOpts.HMACSecret = format.DeriveKeyFromMasterKey(ufb.FormatEncryptionKey, ufb.KopiaRepository.UniqueID, localCacheIntegrityPurpose, localCacheIntegrityHMACSecretLength)
	}

	fo := &ufb.RepoConfig.ContentFormat

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
		ufb.CacheMTime, cmOpts.TimeNow, options.OnFatalError, options.TestOnlyIgnoreMissingRequiredFeatures)

	fop, err := format.NewFormattingOptionsProvider(fo, cmOpts.RepositoryFormatBytes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create format options provider")
	}

	scm, err := content.NewSharedManager(ctx, st, fop, cacheOpts, cmOpts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create shared content manager")
	}

	cm := content.NewWriteManager(ctx, scm, content.SessionOptions{
		SessionUser: lc.Username,
		SessionHost: lc.Hostname,
	}, "")

	om, err := object.NewObjectManager(ctx, cm, ufb.RepoConfig.ObjectFormat)
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
			uniqueID:            ufb.KopiaRepository.UniqueID,
			cachingOptions:      *cacheOpts,
			formatBlob:          ufb.KopiaRepository,
			blobCfgBlob:         blobcfg,
			formatEncryptionKey: ufb.FormatEncryptionKey,
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

func handleMissingRequiredFeatures(ctx context.Context, repoConfig *format.RepositoryConfig, ignoreErrors bool) error {
	// See if the current version of Kopia supports all features required by the repository format.
	// so we can safely fail to start in case repository has been upgraded to a new, incompatible version.
	if missingFeatures := feature.GetUnsupportedFeatures(repoConfig.RequiredFeatures, supportedFeatures); len(missingFeatures) > 0 {
		for _, mf := range missingFeatures {
			if ignoreErrors || mf.IfNotUnderstood.Warn {
				log(ctx).Warnf("%s", mf.UnsupportedMessage())
			} else {
				// by default, fail hard
				return errors.Errorf("%s", mf.UnsupportedMessage())
			}
		}
	}

	return nil
}

func wrapLockingStorage(st blob.Storage, r format.BlobStorageConfiguration) blob.Storage {
	// collect prefixes that need to be locked on put
	var prefixes []string
	for _, prefix := range content.PackBlobIDPrefixes {
		prefixes = append(prefixes, string(prefix))
	}

	prefixes = append(prefixes, content.LegacyIndexBlobPrefix, epoch.EpochManagerIndexUberPrefix, format.KopiaRepositoryBlobID,
		format.KopiaBlobCfgBlobID)

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
	onFatalError func(err error),
	ignoreMissingRequiredFeatures bool,
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

		ufb, err := format.ReadAndCacheDecodedRepositoryConfig(ctx, st, password, cacheOpts.CacheDirectory, lockRefreshInterval)
		if err != nil {
			// nolint:wrapcheck
			return err
		}

		if err := handleMissingRequiredFeatures(ctx, ufb.RepoConfig, ignoreMissingRequiredFeatures); err != nil {
			onFatalError(err)
			return err
		}

		// only allow the upgrade owner to perform storage operations
		if locked, _ := ufb.RepoConfig.UpgradeLock.IsLocked(now()); locked && upgradeOwnerID != ufb.RepoConfig.UpgradeLock.OwnerID {
			return ErrRepositoryUnavailableDueToUpgrageInProgress
		}

		// prevent backward jumps on nextSync
		newNextSync := ufb.CacheMTime.Add(lockRefreshInterval)
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
