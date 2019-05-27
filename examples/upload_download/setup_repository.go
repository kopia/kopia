//+build !test

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/block"
	"github.com/kopia/kopia/repo/storage/filesystem"
	"github.com/kopia/kopia/repo/storage/logging"
)

const (
	masterPassword = "my-password$!@#!@"
	storageDir     = "/tmp/kopia-example/storage"
	configFile     = "/tmp/kopia-example/config"
	cacheDirectory = "/tmp/kopia-example/cache"
)

func setupRepositoryAndConnect(ctx context.Context, password string) error {
	if err := os.MkdirAll(storageDir, 0700); err != nil {
		return fmt.Errorf("unable to create directory: %v", err)
	}
	st, err := filesystem.New(ctx, &filesystem.Options{
		Path: storageDir,
	})
	if err != nil {
		return fmt.Errorf("unable to connect to storage: %v", err)
	}

	// set up logging so we can see what's going on
	st = logging.NewWrapper(st)

	// see if we already have the config file, if not connect.
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		// initialize repository
		if err := repo.Initialize(ctx, st, &repo.NewRepositoryOptions{}, password); err != nil {
			return fmt.Errorf("unable to initialize repository: %v", err)
		}

		// now establish connection to repository and create configuration file.
		if err := repo.Connect(ctx, configFile, st, password, repo.ConnectOptions{
			CachingOptions: block.CachingOptions{
				CacheDirectory:    cacheDirectory,
				MaxCacheSizeBytes: 100000000,
			},
		}); err != nil {
			return fmt.Errorf("unable to connect to repository: %v", err)
		}
	}

	return nil
}
