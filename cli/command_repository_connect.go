package cli

import (
	"context"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

var (
	connectCommand                = repositoryCommands.Command("connect", "Connect to a repository.")
	connectPersistCredentials     bool
	connectCacheDirectory         string
	connectMaxCacheSizeMB         int64
	connectMaxMetadataCacheSizeMB int64
	connectMaxListCacheDuration   time.Duration
	connectHostname               string
	connectUsername               string
	connectCheckForUpdates        bool
	connectReadonly               bool
	connectDescription            string
	connectEnableActions          bool
)

func setupConnectOptions(cmd *kingpin.CmdClause) {
	// Set up flags shared between 'create' and 'connect'. Note that because those flags are used by both command
	// we must use *Var() methods, otherwise one of the commands would always get default flag values.
	cmd.Flag("persist-credentials", "Persist credentials").Default("true").BoolVar(&connectPersistCredentials)
	cmd.Flag("cache-directory", "Cache directory").PlaceHolder("PATH").StringVar(&connectCacheDirectory)
	cmd.Flag("content-cache-size-mb", "Size of local content cache").PlaceHolder("MB").Default("5000").Int64Var(&connectMaxCacheSizeMB)
	cmd.Flag("metadata-cache-size-mb", "Size of local metadata cache").PlaceHolder("MB").Default("5000").Int64Var(&connectMaxMetadataCacheSizeMB)
	cmd.Flag("max-list-cache-duration", "Duration of index cache").Default("30s").Hidden().DurationVar(&connectMaxListCacheDuration)
	cmd.Flag("override-hostname", "Override hostname used by this repository connection").Hidden().StringVar(&connectHostname)
	cmd.Flag("override-username", "Override username used by this repository connection").Hidden().StringVar(&connectUsername)
	cmd.Flag("check-for-updates", "Periodically check for Kopia updates on GitHub").Default("true").Envar(checkForUpdatesEnvar).BoolVar(&connectCheckForUpdates)
	cmd.Flag("readonly", "Make repository read-only to avoid accidental changes").BoolVar(&connectReadonly)
	cmd.Flag("description", "Human-readable description of the repository").StringVar(&connectDescription)
	cmd.Flag("enable-actions", "Allow snapshot actions").BoolVar(&connectEnableActions)
}

func connectOptions() *repo.ConnectOptions {
	return &repo.ConnectOptions{
		PersistCredentials: connectPersistCredentials,
		CachingOptions: content.CachingOptions{
			CacheDirectory:            connectCacheDirectory,
			MaxCacheSizeBytes:         connectMaxCacheSizeMB << 20,         //nolint:gomnd
			MaxMetadataCacheSizeBytes: connectMaxMetadataCacheSizeMB << 20, //nolint:gomnd
			MaxListCacheDurationSec:   int(connectMaxListCacheDuration.Seconds()),
		},
		ClientOptions: repo.ClientOptions{
			Hostname:      connectHostname,
			Username:      connectUsername,
			ReadOnly:      connectReadonly,
			Description:   connectDescription,
			EnableActions: connectEnableActions,
		},
	}
}

func init() {
	setupConnectOptions(connectCommand)
}

func runConnectCommandWithStorage(ctx context.Context, st blob.Storage) error {
	password, err := getPasswordFromFlags(ctx, false, false)
	if err != nil {
		return errors.Wrap(err, "getting password")
	}

	return runConnectCommandWithStorageAndPassword(ctx, st, password)
}

func runConnectCommandWithStorageAndPassword(ctx context.Context, st blob.Storage, password string) error {
	configFile := repositoryConfigFileName()
	if err := repo.Connect(ctx, configFile, st, password, connectOptions()); err != nil {
		return errors.Wrap(err, "error connecting to repository")
	}

	log(ctx).Infof("Connected to repository.")
	maybeInitializeUpdateCheck(ctx)

	return nil
}
