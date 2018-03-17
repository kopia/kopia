package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/kopia/kopia/block"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/storage"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	connectCommand                = repositoryCommands.Command("connect", "Connect to a repository.")
	connectDontPersistCredentials bool
	connectCacheDirectory         string
	connectMaxCacheSizeMB         int64
	connectMaxListCacheDuration   time.Duration
)

func setupConnectOptions(cmd *kingpin.CmdClause) {
	// Set up flags shared between 'create' and 'connect'. Note that because those flags are used by both command
	// we must use *Var() methods, otherwise one of the commands would always get default flag values.
	cmd.Flag("no-credentials", "Don't save credentials in the configuration file").Short('n').BoolVar(&connectDontPersistCredentials)
	cmd.Flag("cache-directory", "Cache directory").PlaceHolder("PATH").StringVar(&connectCacheDirectory)
	cmd.Flag("cache-size-mb", "Size of local cache").PlaceHolder("MB").Default("500").Int64Var(&connectMaxCacheSizeMB)
	cmd.Flag("max-list-cache-duration", "Duration of index cache").Default("600s").Hidden().DurationVar(&connectMaxListCacheDuration)
}

func connectOptions() repo.ConnectOptions {
	return repo.ConnectOptions{
		PersistCredentials: !connectDontPersistCredentials,
		CachingOptions: block.CachingOptions{
			CacheDirectory:          connectCacheDirectory,
			MaxCacheSizeBytes:       connectMaxCacheSizeMB << 20,
			MaxListCacheDurationSec: int(connectMaxListCacheDuration.Seconds()),
		},
	}
}

func init() {
	setupConnectOptions(connectCommand)
}

func runConnectCommandWithStorage(st storage.Storage) error {
	creds, err := getRepositoryCredentials(false)
	if err != nil {
		return err
	}

	err = repo.Connect(context.Background(), repositoryConfigFileName(), st, creds, connectOptions())
	if err != nil {
		return err
	}

	fmt.Fprintln(os.Stderr, "Connected to repository")

	return err
}
