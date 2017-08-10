package cli

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/repo"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	connectCommand                = app.Command("connect", "Connect to a repository.")
	connectRepositoryLocation     = connectCommand.Arg("location", "Repository address").Required().String()
	connectDontPersistCredentials bool
	connectCacheDirectory         string

	// options shared by various providers
	connectCredentialsFile string
	connectReadOnly        bool

	// options for filesystem provider
	connectOwnerUID string
	connectOwnerGID string
	connectFileMode string
	connectDirMode  string
)

func setupConnectOptions(cmd *kingpin.CmdClause) {
	// Set up flags shared between 'create' and 'connect'. Note that because those flags are used by both command
	// we must use *Var() methods, otherwise one of the commands would always get default flag values.
	cmd.Flag("no-credentials", "Don't save credentials in the configuration file").Short('n').BoolVar(&connectDontPersistCredentials)
	cmd.Flag("cache-directory", "Cache directory").PlaceHolder("PATH").StringVar(&connectCacheDirectory)
	cmd.Flag("credentials", "File containing credentials to connect to storage (GCS)").PlaceHolder("PATH").ExistingFileVar(&connectCredentialsFile)
	cmd.Flag("read-only", "Connect in read-only mode").BoolVar(&connectReadOnly)

	cmd.Flag("owner-uid", "User ID owning newly created files").PlaceHolder("USER").StringVar(&connectOwnerUID)
	cmd.Flag("owner-gid", "Group ID owning newly created files").PlaceHolder("GROUP").StringVar(&connectOwnerGID)
	cmd.Flag("file-mode", "File mode for newly created files (0600)").PlaceHolder("MODE").StringVar(&connectFileMode)
	cmd.Flag("dir-mode", "Mode of newly directory files (0700)").PlaceHolder("MODE").StringVar(&connectDirMode)
}

func connectOptions() repo.ConnectOptions {
	return repo.ConnectOptions{
		PersistCredentials: !connectDontPersistCredentials,
		CacheDirectory:     connectCacheDirectory,
	}
}

func init() {
	setupConnectOptions(connectCommand)
	connectCommand.Action(runConnectCommand)
}

func runConnectCommand(_ *kingpin.ParseContext) error {
	storage, err := newStorageFromURL(getContext(), *connectRepositoryLocation)
	if err != nil {
		return err
	}

	creds, err := getRepositoryCredentials(false)
	if err != nil {
		return err
	}

	if err := repo.Connect(context.Background(), repositoryConfigFileName(), storage, creds, connectOptions()); err != nil {
		return err
	}

	fmt.Println("Connected to repository:", *connectRepositoryLocation)

	return err
}
