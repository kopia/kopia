package main

import (
	"context"
	"fmt"
	"os"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/storage/gcs"
	"github.com/kopia/kopia/storage/logging"
)

const (
	masterPassword = "my-password$!@#!@"
	bucketName     = "kopia-example"
	configFile     = "/tmp/kopia-example/config"
	cacheDirectory = "/tmp/kopia-example/cache"
)

func setupRepositoryAndConnect(ctx context.Context, creds auth.Credentials) error {
	st, err := gcs.New(ctx, &gcs.Options{
		BucketName: bucketName,
	})
	if err != nil {
		return fmt.Errorf("unable to connect to storage: %v", err)
	}

	// set up logging so we can see what's going on
	st = logging.NewWrapper(st)

	// see if we already have the config file, if not connect.
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		// initialize repository
		if err := repo.Initialize(ctx, st, &repo.NewRepositoryOptions{}, creds); err != nil {
			log.Errorf("unable to initialize repository: %v", err)
		}

		// now establish connection to repository and create configuration file.
		if err := repo.Connect(ctx, configFile, st, creds, repo.ConnectOptions{
			PersistCredentials: false,
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
