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

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/cacheprot"
	"github.com/kopia/kopia/internal/crypto"
	"github.com/kopia/kopia/internal/feature"
	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/internal/repodiag"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/beforeop"
	loggingwrapper "github.com/kopia/kopia/repo/blob/logging"
	"github.com/kopia/kopia/repo/blob/readonly"
	"github.com/kopia/kopia/repo/blob/storagemetrics"
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
//
//nolint:gochecknoglobals
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

//nolint:gochecknoglobals
var localCacheIntegrityPurpose = []byte("local-cache-integrity")

var log = logging.Module("kopia/repo")

// Options provides configuration parameters for connection to a repository.
type Options struct {
	TraceStorage        bool                       // Logs all storage access using provided Printf-style function
	TimeNowFunc         func() time.Time           // Time provider
	DisableInternalLog  bool                       // Disable internal log
	UpgradeOwnerID      string                     // Owner-ID of any upgrade in progress, when this is not set the access may be restricted
	DoNotWaitForUpgrade bool                       // Disable the exponential forever backoff on an upgrade lock.
	BeforeFlush         []RepositoryWriterCallback // list of callbacks to invoke before every flush

	OnFatalError func(err error) // function to invoke when repository encounters a fatal error, usually invokes os.Exit

	// test-only flags
	TestOnlyIgnoreMissingRequiredFeatures bool // ignore missing features
}

// ErrInvalidPassword is returned when repository password is invalid.
var ErrInvalidPassword = format.ErrInvalidPassword

// ErrAlreadyInitialized is returned when repository is already initialized in the provided storage.
var ErrAlreadyInitialized = format.ErrAlreadyInitialized

// ErrRepositoryUnavailableDueToUpgradeInProgress is returned when repository
// is undergoing upgrade that requires exclusive access.
var ErrRepositoryUnavailableDueToUpgradeInProgress = errors.New("repository upgrade in progress")

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

	if lc.PermissiveCacheLoading && !lc.ReadOnly {
		return nil, ErrCannotWriteToRepoConnectionWithPermissiveCacheLoading
	}

	if lc.APIServer != nil {
		return openAPIServer(ctx, lc.APIServer, lc.ClientOptions, lc.Caching, password, options)
	}

	return openDirect(ctx, configFile, lc, password, options)
}

func getContentCacheOrNil(ctx context.Context, si *APIServerInfo, opt *content.CachingOptions, password string, mr *metrics.Registry, timeNow func() time.Time) (*cache.PersistentCache, error) {
	opt = opt.CloneOrDefault()

	cs, err := cache.NewStorageOrNil(ctx, opt.CacheDirectory, opt.ContentCacheSizeBytes, "server-contents")
	if cs == nil {
		// this may be (nil, nil) or (nil, err)
		return nil, errors.Wrap(err, "error opening storage")
	}

	// derive content cache key from the password & HMAC secret
	saltWithPurpose := append([]byte("content-cache-protection"), opt.HMACSecret...)

	const cacheEncryptionKeySize = 32

	keyAlgo := si.LocalCacheKeyDerivationAlgorithm
	if keyAlgo == "" {
		keyAlgo = DefaultServerRepoCacheKeyDerivationAlgorithm
	}

	cacheEncryptionKey, err := crypto.DeriveKeyFromPassword(password, saltWithPurpose, cacheEncryptionKeySize, keyAlgo)
	if err != nil {
		return nil, errors.Wrap(err, "unable to derive cache encryption key from password")
	}

	prot, err := cacheprot.AuthenticatedEncryptionProtection(cacheEncryptionKey)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize protection")
	}

	pc, err := cache.NewPersistentCache(ctx, "cache-storage", cs, prot, cache.SweepSettings{
		MaxSizeBytes: opt.ContentCacheSizeBytes,
		LimitBytes:   opt.ContentCacheSizeLimitBytes,
		MinSweepAge:  opt.MinContentSweepAge.DurationOrDefault(content.DefaultDataCacheSweepAge),
	}, mr, timeNow)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open persistent cache")
	}

	return pc, nil
}

// openAPIServer connects remote repository over Kopia API.
func openAPIServer(ctx context.Context, si *APIServerInfo, cliOpts ClientOptions, cachingOptions *content.CachingOptions, password string, options *Options) (Repository, error) {
	cachingOptions = cachingOptions.CloneOrDefault()

	mr := metrics.NewRegistry()

	contentCache, err := getContentCacheOrNil(ctx, si, cachingOptions, password, mr, options.TimeNowFunc)
	if err != nil {
		return nil, errors.Wrap(err, "error opening content cache")
	}

	closer := newRefCountedCloser(
		func(ctx context.Context) error {
			if contentCache != nil {
				contentCache.Close(ctx)
			}

			return nil
		},
		mr.Close,
	)

	par := &immutableServerRepositoryParameters{
		cliOpts:          cliOpts,
		contentCache:     contentCache,
		metricsRegistry:  mr,
		refCountedCloser: closer,
		beforeFlush:      options.BeforeFlush,
	}

	return openGRPCAPIRepository(ctx, si, password, par)
}

// openDirect opens the repository that directly manipulates blob storage..
func openDirect(ctx context.Context, configFile string, lc *LocalConfig, password string, options *Options) (rep Repository, err error) {
	if lc.Storage == nil {
		return nil, errors.New("storage not set in the configuration file")
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

	cliOpts := lc.ApplyDefaults(ctx, "Repository in "+st.DisplayName())

	r, err := openWithConfig(ctx, st, cliOpts, password, options, lc.Caching, configFile)
	if err != nil {
		st.Close(ctx) //nolint:errcheck
		return nil, err
	}

	return r, nil
}

// openWithConfig opens the repository with a given configuration, avoiding the need for a config file.
//
//nolint:funlen,gocyclo
func openWithConfig(ctx context.Context, st blob.Storage, cliOpts ClientOptions, password string, options *Options, cacheOpts *content.CachingOptions, configFile string) (DirectRepository, error) {
	cacheOpts = cacheOpts.CloneOrDefault()
	cmOpts := &content.ManagerOptions{
		TimeNow:                defaultTime(options.TimeNowFunc),
		DisableInternalLog:     options.DisableInternalLog,
		PermissiveCacheLoading: cliOpts.PermissiveCacheLoading,
	}

	mr := metrics.NewRegistry()
	st = storagemetrics.NewWrapper(st, mr)

	fmgr, ferr := format.NewManager(ctx, st, cacheOpts.CacheDirectory, cliOpts.FormatBlobCacheDuration, password, cmOpts.TimeNow)
	if ferr != nil {
		return nil, errors.Wrap(ferr, "unable to create format manager")
	}

	// check features before and perform configuration before performing IO
	if err := handleMissingRequiredFeatures(ctx, fmgr, options.TestOnlyIgnoreMissingRequiredFeatures); err != nil {
		return nil, err
	}

	if fmgr.SupportsPasswordChange() {
		cacheOpts.HMACSecret = crypto.DeriveKeyFromMasterKey(fmgr.GetHmacSecret(), fmgr.UniqueID(), localCacheIntegrityPurpose, localCacheIntegrityHMACSecretLength)
	} else {
		// deriving from ufb.FormatEncryptionKey was actually a bug, that only matters will change when we change the password
		cacheOpts.HMACSecret = crypto.DeriveKeyFromMasterKey(fmgr.FormatEncryptionKey(), fmgr.UniqueID(), localCacheIntegrityPurpose, localCacheIntegrityHMACSecretLength)
	}

	limits := throttlingLimitsFromConnectionInfo(ctx, st.ConnectionInfo())
	if cliOpts.Throttling != nil {
		limits = *cliOpts.Throttling
	}

	st, throttler, ferr := addThrottler(st, limits)
	if ferr != nil {
		return nil, errors.Wrap(ferr, "unable to add throttler")
	}

	throttler.OnUpdate(func(l throttling.Limits) error {
		lc2, err2 := LoadConfigFromFile(configFile)
		if err2 != nil {
			return err2
		}

		lc2.Throttling = &l

		return lc2.writeToFile(configFile)
	})

	blobcfg, err := fmgr.BlobCfgBlob(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "blob configuration")
	}

	if blobcfg.IsRetentionEnabled() {
		st = wrapLockingStorage(st, blobcfg)
	}

	_, err = retry.WithExponentialBackoffMaxRetries(ctx, -1, "wait for upgrade", func() (interface{}, error) {
		uli, err := fmgr.UpgradeLockIntent(ctx)
		if err != nil {
			//nolint:wrapcheck
			return nil, err
		}

		// retry if upgrade lock has been taken
		if !cliOpts.PermissiveCacheLoading {
			if locked, _ := uli.IsLocked(cmOpts.TimeNow()); locked && options.UpgradeOwnerID != uli.OwnerID {
				return nil, ErrRepositoryUnavailableDueToUpgradeInProgress
			}
		}

		return false, nil
	}, func(internalErr error) bool {
		return !options.DoNotWaitForUpgrade && errors.Is(internalErr, ErrRepositoryUnavailableDueToUpgradeInProgress)
	})
	if err != nil {
		return nil, err
	}

	if !cliOpts.PermissiveCacheLoading {
		// background/interleaving upgrade lock storage monitor
		st = upgradeLockMonitor(fmgr, options.UpgradeOwnerID, st, cmOpts.TimeNow, options.OnFatalError, options.TestOnlyIgnoreMissingRequiredFeatures)
	}

	dw := repodiag.NewWriter(st, fmgr)
	logManager := repodiag.NewLogManager(ctx, dw)

	scm, ferr := content.NewSharedManager(ctx, st, fmgr, cacheOpts, cmOpts, logManager, mr)
	if ferr != nil {
		return nil, errors.Wrap(ferr, "unable to create shared content manager")
	}

	cm := content.NewWriteManager(ctx, scm, content.SessionOptions{
		SessionUser: cliOpts.Username,
		SessionHost: cliOpts.Hostname,
	}, "")

	om, ferr := object.NewObjectManager(ctx, cm, fmgr.ObjectFormat(), mr)
	if ferr != nil {
		return nil, errors.Wrap(ferr, "unable to open object manager")
	}

	manifests, ferr := manifest.NewManager(ctx, cm, manifest.ManagerOptions{TimeNow: cmOpts.TimeNow}, mr)
	if ferr != nil {
		return nil, errors.Wrap(ferr, "unable to open manifests")
	}

	closer := newRefCountedCloser(
		scm.CloseShared,
		dw.Wait,
		mr.Close,
		st.Close,
	)

	dr := &directRepository{
		cmgr:  cm,
		omgr:  om,
		blobs: st,
		mmgr:  manifests,
		sm:    scm,
		immutableDirectRepositoryParameters: immutableDirectRepositoryParameters{
			cachingOptions:   *cacheOpts,
			fmgr:             fmgr,
			timeNow:          cmOpts.TimeNow,
			cliOpts:          cliOpts,
			configFile:       configFile,
			nextWriterID:     new(int32),
			throttler:        throttler,
			metricsRegistry:  mr,
			refCountedCloser: closer,
			beforeFlush:      options.BeforeFlush,
		},
	}

	return dr, nil
}

func handleMissingRequiredFeatures(ctx context.Context, fmgr *format.Manager, ignoreErrors bool) error {
	required, err := fmgr.RequiredFeatures(ctx)
	if err != nil {
		return errors.Wrap(err, "required features")
	}

	// See if the current version of Kopia supports all features required by the repository format.
	// so we can safely fail to start in case repository has been upgraded to a new, incompatible version.
	if missingFeatures := feature.GetUnsupportedFeatures(required, supportedFeatures); len(missingFeatures) > 0 {
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
	prefixes := GetLockingStoragePrefixes()

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
	fmgr *format.Manager,
	upgradeOwnerID string,
	st blob.Storage,
	now func() time.Time,
	onFatalError func(err error),
	ignoreMissingRequiredFeatures bool,
) blob.Storage {
	var (
		m             sync.RWMutex
		lastCheckTime time.Time
	)

	cb := func(ctx context.Context) error {
		m.RLock()
		// see if we already checked that revision
		if lastCheckTime.Equal(fmgr.LoadedTime()) {
			m.RUnlock()
			return nil
		}
		m.RUnlock()

		// upgrade the lock and verify again in-case someone else won the race to refresh
		m.Lock()
		defer m.Unlock()

		ltime := fmgr.LoadedTime()

		if lastCheckTime.Equal(ltime) {
			return nil
		}

		uli, err := fmgr.UpgradeLockIntent(ctx)
		if err != nil {
			return errors.Wrap(err, "upgrade lock intent")
		}

		if err := handleMissingRequiredFeatures(ctx, fmgr, ignoreMissingRequiredFeatures); err != nil {
			onFatalError(err)
			return err
		}

		if uli != nil {
			// only allow the upgrade owner to perform storage operations
			if locked, _ := uli.IsLocked(now()); locked && upgradeOwnerID != uli.OwnerID {
				return ErrRepositoryUnavailableDueToUpgradeInProgress
			}
		}

		lastCheckTime = ltime

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
