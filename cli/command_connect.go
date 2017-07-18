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
	connectDontPersistCredentials *bool
	connectCacheDirectory         *string
)

func setupConnectOptions(cmd *kingpin.CmdClause) {
	connectDontPersistCredentials = cmd.Flag("no-credentials", "Don't save credentials in the configuration file").Short('n').Bool()
	connectCacheDirectory = cmd.Flag("cache-directory", "Cache directory").PlaceHolder("PATH").String()
}

func connectOptions() repo.ConnectOptions {
	return repo.ConnectOptions{
		PersistCredentials: !*connectDontPersistCredentials,
		CacheDirectory:     *connectCacheDirectory,
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
