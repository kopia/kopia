// Command repository_api demonstrates the use of Kopia's Repository API.
package main

import (
	"context"
	"os"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/repo"
	colorable "github.com/mattn/go-colorable"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	ctx := context.Background()
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: colorable.NewColorableStderr()}).With().Timestamp().Logger()

	// set up credentials
	creds, crederr := auth.Password(masterPassword)
	if crederr != nil {
		log.Fatal().Msgf("invalid password: %v", crederr)
	}

	if err := setupRepositoryAndConnect(ctx, creds); err != nil {
		log.Error().Msgf("unable to set up repository: %v", err)
		os.Exit(1)
	}

	r, err := repo.Open(ctx, configFile, &repo.Options{
		Credentials: creds,
	})
	if err != nil {
		log.Error().Msgf("unable to open repository: %v", err)
		os.Exit(1)
	}
	defer r.Close(ctx) //nolint:errcheck

	uploadAndDownloadObjects(ctx, r)

	// Now list blocks found in the repository.
	blks, err := r.Blocks.ListBlocks("")
	if err != nil {
		log.Fatal().Msgf("err: %v")
	}

	for _, b := range blks {
		log.Printf("found block %v", b)
	}
}
