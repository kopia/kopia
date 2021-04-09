package repo

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/crypto/scrypt"

	"github.com/kopia/kopia/internal/atomicfile"
	"github.com/kopia/kopia/internal/cache"
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

// refresh indexes every 15 minutes while the repository remains open.
const backgroundRefreshInterval = 15 * time.Minute

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
	TraceStorage func(f string, args ...interface{}) // Logs all storage access using provided Printf-style function
	TimeNowFunc  func() time.Time                    // Time provider
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

	cacheEncryptionKey, err := scrypt.Key([]byte(password), salt, 65536, 8, 1, 32)
	if err != nil {
		return nil, errors.Wrap(err, "unable to derive cache encryption key from password")
	}

	prot, err := cache.AuthenticatedEncryptionProtection(cacheEncryptionKey)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize protection")
	}

	return cache.NewPersistentCache(ctx, "cache-storage", cs, prot, opt.MaxCacheSizeBytes, cache.DefaultTouchThreshold, cache.DefaultSweepFrequency)
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
	fb, err := readAndCacheFormatBlobBytes(ctx, st, caching.CacheDirectory)
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

	masterKey, err := f.deriveMasterKeyFromPassword(password)
	if err != nil {
		return nil, err
	}

	repoConfig, err := f.decryptFormatBytes(masterKey)
	if err != nil {
		// nolint:wrapcheck
		return nil, ErrInvalidPassword
	}

	caching.HMACSecret = deriveKeyFromMasterKey(masterKey, f.UniqueID, []byte("local-cache-integrity"), 16)

	fo := &repoConfig.FormattingOptions

	if fo.MaxPackSize == 0 {
		// legacy only, apply default
		fo.MaxPackSize = 20 << 20 // nolint:gomnd
	}

	cmOpts := &content.ManagerOptions{
		RepositoryFormatBytes: fb,
		TimeNow:               defaultTime(options.TimeNowFunc),
	}

	scm, err := content.NewSharedManager(ctx, st, fo, caching, cmOpts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create shared content manager")
	}

	cm := content.NewWriteManager(scm, content.SessionOptions{
		SessionUser: lc.Username,
		SessionHost: lc.Hostname,
	})

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
			uniqueID:       f.UniqueID,
			cachingOptions: *caching,
			formatBlob:     f,
			masterKey:      masterKey,
			timeNow:        cmOpts.TimeNow,
			cliOpts:        lc.ClientOptions.ApplyDefaults(ctx, "Repository in "+st.DisplayName()),
			configFile:     configFile,
		},
		closed: make(chan struct{}),
	}

	go dr.RefreshPeriodically(ctx, backgroundRefreshInterval)

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

	return f.Close()
}

func readAndCacheFormatBlobBytes(ctx context.Context, st blob.Storage, cacheDirectory string) ([]byte, error) {
	cachedFile := filepath.Join(cacheDirectory, "kopia.repository")

	if cacheDirectory != "" {
		if err := os.MkdirAll(cacheDirectory, 0o700); err != nil && !os.IsExist(err) {
			log(ctx).Errorf("unable to create cache directory: %v", err)
		}

		b, err := ioutil.ReadFile(cachedFile) //nolint:gosec
		if err == nil {
			// read from cache.
			return b, nil
		}
	}

	b, err := st.GetBlob(ctx, FormatBlobID, 0, -1)
	if err != nil {
		return nil, errors.Wrap(err, "error getting format blob")
	}

	if cacheDirectory != "" {
		if err := atomicfile.Write(cachedFile, bytes.NewReader(b)); err != nil {
			log(ctx).Errorf("warning: unable to write cache: %v", err)
		}
	}

	return b, nil
}
