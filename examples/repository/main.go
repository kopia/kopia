// Command repository_api demonstrates the use of Kopia's Repository API.
package main

import (
	"context"
	"os"

	"github.com/kopia/kopia/internal/kopialogging"
	"github.com/kopia/kopia/repo"
)

var log = kopialogging.Logger("kopia/example")

func main() {
	ctx := context.Background()

	if err := setupRepositoryAndConnect(ctx, masterPassword); err != nil {
		log.Errorf("unable to set up repository: %v", err)
		os.Exit(1)
	}

	r, err := repo.Open(ctx, configFile, masterPassword, nil)
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
