package repo

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/crypto/scrypt"

	"github.com/kopia/kopia/internal/atomicfile"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	loggingwrapper "github.com/kopia/kopia/repo/blob/logging"
	"github.com/kopia/kopia/repo/blob/readonly"
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

// defaultFormatBlobCacheDuration is the duration for which we treat cached kopia.repository
// as valid.
const defaultFormatBlobCacheDuration = 15 * time.Minute

// localCacheIntegrityHMACSecretLength length of HMAC secret protecting local cache items.
const localCacheIntegrityHMACSecretLength = 16

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

var log = logging.GetContextLoggerFunc("kopia/repo")

// Options provides configuration parameters for connection to a repository.
type Options struct {
	TraceStorage       func(f string, args ...interface{}) // Logs all storage access using provided Printf-style function
	TimeNowFunc        func() time.Time                    // Time provider
	DisableInternalLog bool                                // Disable internal log
}

// ErrInvalidPassword is returned when repository password is invalid.
var ErrInvalidPassword = errors.Errorf("invalid repository password")

// Open opens a Repository specified in the configuration file.
func Open(ctx context.Context, configFile, password string, options *Options) (rep Repository, err error) {
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

	pc, err := cache.NewPersistentCache(ctx, "cache-storage", cs, prot, opt.MaxCacheSizeBytes, cache.DefaultTouchThreshold, cache.DefaultSweepFrequency)
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

	st, err := blob.NewStorage(ctx, *lc.Storage)
	if err != nil {
		return nil, errors.Wrap(err, "cannot open storage")
	}

	if options.TraceStorage != nil {
		st = loggingwrapper.NewWrapper(st, options.TraceStorage, "[STORAGE] ")
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
func openWithConfig(ctx context.Context, st blob.Storage, lc *LocalConfig, password string, options *Options, caching *content.CachingOptions, configFile string) (DirectRepository, error) {
	caching = caching.CloneOrDefault()

	// Read format blob, potentially from cache.
	fb, err := readAndCacheFormatBlobBytes(ctx, st, caching.CacheDirectory, lc.FormatBlobCacheDuration)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read format blob")
	}

	if err = writeCacheMarker(caching.CacheDirectory); err != nil {
		return nil, errors.Wrap(err, "unable to write cache directory marker")
	}

	f, err := parseFormatBlob(fb)
	if err != nil {
		return nil, errors.Wrap(err, "can't parse format blob")
	}

	fb, err = addFormatBlobChecksumAndLength(fb)
	if err != nil {
		return nil, errors.Errorf("unable to add checksum")
	}

	formatEncryptionKey, err := f.deriveFormatEncryptionKeyFromPassword(password)
	if err != nil {
		return nil, err
	}

	repoConfig, err := f.decryptFormatBytes(formatEncryptionKey)
	if err != nil {
		return nil, ErrInvalidPassword
	}

	if repoConfig.FormattingOptions.EnablePasswordChange {
		caching.HMACSecret = deriveKeyFromMasterKey(repoConfig.HMACSecret, f.UniqueID, localCacheIntegrityPurpose, localCacheIntegrityHMACSecretLength)
	} else {
		// deriving from formatEncryptionKey was actually a bug, that only matters will change when we change the password
		caching.HMACSecret = deriveKeyFromMasterKey(formatEncryptionKey, f.UniqueID, localCacheIntegrityPurpose, localCacheIntegrityHMACSecretLength)
	}

	fo := &repoConfig.FormattingOptions

	if fo.MaxPackSize == 0 {
		// legacy only, apply default
		fo.MaxPackSize = 20 << 20 // nolint:gomnd
	}

	cmOpts := &content.ManagerOptions{
		RepositoryFormatBytes: fb,
		TimeNow:               defaultTime(options.TimeNowFunc),
		DisableInternalLog:    options.DisableInternalLog,
	}

	// do not embed repository format info in pack blobs when password change is enabled.
	if fo.EnablePasswordChange {
		cmOpts.RepositoryFormatBytes = nil
	}

	scm, err := content.NewSharedManager(ctx, st, fo, caching, cmOpts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create shared content manager")
	}

	cm := content.NewWriteManager(ctx, scm, content.SessionOptions{
		SessionUser: lc.Username,
		SessionHost: lc.Hostname,
	}, "")

	om, err := object.NewObjectManager(ctx, cm, repoConfig.Format)
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
			uniqueID:            f.UniqueID,
			cachingOptions:      *caching,
			formatBlob:          f,
			formatEncryptionKey: formatEncryptionKey,
			timeNow:             cmOpts.TimeNow,
			cliOpts:             lc.ClientOptions.ApplyDefaults(ctx, "Repository in "+st.DisplayName()),
			configFile:          configFile,
			nextWriterID:        new(int32),
		},
		closed: make(chan struct{}),
	}

	return dr, nil
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

	f, err := os.Create(markerFile)
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

func readFormatBlobBytesFromCache(ctx context.Context, cachedFile string, validDuration time.Duration) ([]byte, error) {
	cst, err := os.Stat(cachedFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open cache file")
	}

	if clock.Since(cst.ModTime()) > validDuration {
		// got cached file, but it's too old, remove it
		if err := os.Remove(cachedFile); err != nil {
			log(ctx).Debugf("unable to remove cache file: %v", err)
		}

		return nil, errors.Errorf("cached file too old")
	}

	return os.ReadFile(cachedFile) //nolint:wrapcheck
}

func readAndCacheFormatBlobBytes(ctx context.Context, st blob.Storage, cacheDirectory string, validDuration time.Duration) ([]byte, error) {
	cachedFile := filepath.Join(cacheDirectory, "kopia.repository")

	if validDuration == 0 {
		validDuration = defaultFormatBlobCacheDuration
	}

	if cacheDirectory != "" {
		if err := os.MkdirAll(cacheDirectory, cache.DirMode); err != nil && !os.IsExist(err) {
			log(ctx).Errorf("unable to create cache directory: %v", err)
		}
	}

	cacheEnabled := formatBytesCachingEnabled(cacheDirectory, validDuration)
	if cacheEnabled {
		b, err := readFormatBlobBytesFromCache(ctx, cachedFile, validDuration)
		if err == nil {
			log(ctx).Debugf("kopia.repository retrieved from cache")

			return b, nil
		}

		log(ctx).Debugf("kopia.repository could not be fetched from cache: %v", err)
	} else {
		log(ctx).Debugf("kopia.repository cache not enabled")
	}

	var b gather.WriteBuffer
	defer b.Close()

	if err := st.GetBlob(ctx, FormatBlobID, 0, -1, &b); err != nil {
		return nil, errors.Wrap(err, "error getting format blob")
	}

	if cacheEnabled {
		if err := atomicfile.Write(cachedFile, b.Bytes().Reader()); err != nil {
			log(ctx).Errorf("warning: unable to write cache: %v", err)
		}
	}

	return b.ToByteSlice(), nil
}
