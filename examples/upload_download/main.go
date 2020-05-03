//+build !test

// Command repository_api demonstrates the use of Kopia's Repository API.
package main

import (
	"context"
	"log"
	"os"

	"github.com/kopia/kopia/repo"
)

func main() {
	ctx := context.Background()

	if err := setupRepositoryAndConnect(ctx, masterPassword); err != nil {
		log.Printf("unable to set up repository: %v", err)
		os.Exit(1)
	}

	r, err := repo.Open(ctx, configFile, masterPassword, nil)
	if err != nil {
		log.Printf("unable to open repository: %v", err)
		os.Exit(1)
	}
	defer r.Close(ctx) //nolint:errcheck

	uploadAndDownloadObjects(ctx, r)
}
