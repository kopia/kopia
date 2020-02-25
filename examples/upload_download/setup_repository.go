//+build !test

package main

import (
	"context"
	"log"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/blob/logging"
	"github.com/kopia/kopia/repo/content"
)

const (
	masterPassword = "my-password$!@#!@"
	storageDir     = "/tmp/kopia-example/storage"
	configFile     = "/tmp/kopia-example/config"
	cacheDirectory = "/tmp/kopia-example/cache"
)

func setupRepositoryAndConnect(ctx context.Context, password string) error {
	if err := os.MkdirAll(storageDir, 0700); err != nil {
		return errors.Wrap(err, "unable to create directory")
	}

	st, err := filesystem.New(ctx, &filesystem.Options{
		Path: storageDir,
	})
	if err != nil {
		return errors.Wrap(err, "unable to connect to storage")
	}

	// set up logging so we can see what's going on
	st = logging.NewWrapper(st, log.Printf, "")

	// see if we already have the config file, if not connect.
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		// initialize repository
		if err := repo.Initialize(ctx, st, &repo.NewRepositoryOptions{}, password); err != nil {
			return errors.Wrap(err, "unable to initialize repository")
		}

		// now establish connection to repository and create configuration file.
		if err := repo.Connect(ctx, configFile, st, password, &repo.ConnectOptions{
			CachingOptions: content.CachingOptions{
				CacheDirectory:    cacheDirectory,
				MaxCacheSizeBytes: 100000000,
			},
		}); err != nil {
			return errors.Wrap(err, "unable to connect to repository")
		}
	}

	return nil
}
