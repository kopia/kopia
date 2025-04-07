package cli

import (
	"context"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/passwordpersist"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

type commandRepositoryConnect struct {
	co connectOptions

	server commandRepositoryConnectServer
}

func (c *commandRepositoryConnect) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("connect", "Connect to a repository.")

	c.co.setup(svc, cmd)
	c.server.setup(svc, cmd, &c.co)

	for _, prov := range svc.storageProviders() {
		// Set up 'connect' subcommand
		f := prov.NewFlags()
		cc := cmd.Command(prov.Name, "Connect to repository in "+prov.Description)
		f.Setup(svc, cc)
		cc.Action(func(kpc *kingpin.ParseContext) error {
			return svc.runAppWithContext(kpc.SelectedCommand, func(ctx context.Context) error {
				st, err := f.Connect(ctx, false, 0)
				if err != nil {
					return errors.Wrap(err, "can't connect to storage")
				}

				return svc.runConnectCommandWithStorage(ctx, &c.co, st)
			})
		})
	}
}

type connectOptions struct {
	connectCacheDirectory string

	cacheSizeFlags

	connectHostname               string
	connectUsername               string
	connectCheckForUpdates        bool
	connectReadonly               bool
	connectPermissiveCacheLoading bool
	connectDescription            string
	connectEnableActions          bool

	formatBlobCacheDuration time.Duration
	disableFormatBlobCache  bool
}

func (c *connectOptions) setup(svc appServices, cmd *kingpin.CmdClause) {
	// Set up flags shared between 'create' and 'connect'. Note that because those flags are used by both command
	// we must use *Var() methods, otherwise one of the commands would always get default flag values.
	cmd.Flag("cache-directory", "Cache directory").PlaceHolder("PATH").Envar(svc.EnvName("KOPIA_CACHE_DIRECTORY")).StringVar(&c.connectCacheDirectory)

	c.maxListCacheDuration = 30 * time.Second //nolint:mnd
	c.contentCacheSizeMB = 5000
	c.metadataCacheSizeMB = 5000
	c.cacheSizeFlags.setup(cmd)

	cmd.Flag("override-hostname", "Override hostname used by this repository connection").Hidden().StringVar(&c.connectHostname)
	cmd.Flag("override-username", "Override username used by this repository connection").Hidden().StringVar(&c.connectUsername)
	cmd.Flag("check-for-updates", "Periodically check for Kopia updates on GitHub").Default("true").Envar(svc.EnvName(checkForUpdatesEnvar)).BoolVar(&c.connectCheckForUpdates)
	cmd.Flag("readonly", "Make repository read-only to avoid accidental changes").BoolVar(&c.connectReadonly)
	cmd.Flag("permissive-cache-loading", "Do not fail when loading bad cache index entries.  Repository must be opened in read-only mode").Hidden().BoolVar(&c.connectPermissiveCacheLoading)
	cmd.Flag("description", "Human-readable description of the repository").StringVar(&c.connectDescription)
	cmd.Flag("enable-actions", "Allow snapshot actions").BoolVar(&c.connectEnableActions)
	cmd.Flag("repository-format-cache-duration", "Duration of kopia.repository format blob cache").Hidden().DurationVar(&c.formatBlobCacheDuration)
	cmd.Flag("disable-repository-format-cache", "Disable caching of kopia.repository format blob").Hidden().BoolVar(&c.disableFormatBlobCache)
}

func (c *connectOptions) getFormatBlobCacheDuration() time.Duration {
	if c.disableFormatBlobCache {
		return -1
	}

	return c.formatBlobCacheDuration
}

func (c *connectOptions) toRepoConnectOptions() *repo.ConnectOptions {
	return &repo.ConnectOptions{
		CachingOptions: content.CachingOptions{
			CacheDirectory:              c.connectCacheDirectory,
			ContentCacheSizeBytes:       c.contentCacheSizeMB << 20,       //nolint:mnd
			ContentCacheSizeLimitBytes:  c.contentCacheSizeLimitMB << 20,  //nolint:mnd
			MetadataCacheSizeBytes:      c.metadataCacheSizeMB << 20,      //nolint:mnd
			MetadataCacheSizeLimitBytes: c.metadataCacheSizeLimitMB << 20, //nolint:mnd
			MaxListCacheDuration:        content.DurationSeconds(c.maxListCacheDuration.Seconds()),
			MinContentSweepAge:          content.DurationSeconds(c.contentMinSweepAge.Seconds()),
			MinMetadataSweepAge:         content.DurationSeconds(c.metadataMinSweepAge.Seconds()),
			MinIndexSweepAge:            content.DurationSeconds(c.indexMinSweepAge.Seconds()),
		},
		ClientOptions: repo.ClientOptions{
			Hostname:                c.connectHostname,
			Username:                c.connectUsername,
			ReadOnly:                c.connectReadonly,
			PermissiveCacheLoading:  c.connectPermissiveCacheLoading,
			Description:             c.connectDescription,
			EnableActions:           c.connectEnableActions,
			FormatBlobCacheDuration: c.getFormatBlobCacheDuration(),
		},
	}
}

func (c *App) runConnectCommandWithStorage(ctx context.Context, co *connectOptions, st blob.Storage) error {
	pass, err := c.getPasswordFromFlags(ctx, false, false)
	if err != nil {
		return errors.Wrap(err, "getting password")
	}

	return c.runConnectCommandWithStorageAndPassword(ctx, co, st, pass)
}

func (c *App) runConnectCommandWithStorageAndPassword(ctx context.Context, co *connectOptions, st blob.Storage, password string) error {
	configFile := c.repositoryConfigFileName()
	if err := passwordpersist.OnSuccess(
		ctx, repo.Connect(ctx, configFile, st, password, co.toRepoConnectOptions()),
		c.passwordPersistenceStrategy(), configFile, password); err != nil {
		return errors.Wrap(err, "error connecting to repository")
	}

	log(ctx).Info("Connected to repository.")
	c.maybeInitializeUpdateCheck(ctx, co)

	return nil
}
