// Command repository_api demonstrates the use of Kopia's Repository API.
package main

import (
	"context"
	"os"

	"github.com/kopia/kopia/internal/kopialogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/auth"
)

var log = kopialogging.Logger("kopia/example")

func main() {
	ctx := context.Background()

	// set up credentials
	creds, crederr := auth.Password(masterPassword)
	if crederr != nil {
		log.Fatalf("invalid password: %v", crederr)
	}

	if err := setupRepositoryAndConnect(ctx, creds); err != nil {
		log.Errorf("unable to set up repository: %v", err)
		os.Exit(1)
	}

	r, err := repo.Open(ctx, configFile, &repo.Options{
		Credentials: creds,
	})
	if err != nil {
		log.Errorf("unable to open repository: %v", err)
		os.Exit(1)
	}
	defer r.Close(ctx) //nolint:errcheck

	uploadAndDownloadObjects(ctx, r)

	// Now list blocks found in the repository.
	blks, err := r.Blocks.ListBlocks("")
	if err != nil {
		log.Errorf("err: %v", err)
	}

	for _, b := range blks {
		log.Infof("found block %v", b)
	}
}
