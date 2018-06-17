package cli

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/kopia/kopia/internal/server"
	"github.com/kopia/kopia/repo"
	"github.com/rs/zerolog/log"
)

var (
	serverAddress = serverCommands.Flag("--address", "Server address").Default("127.0.0.1:51515").String()

	serverStartCommand = serverCommands.Command("start", "Start Kopia server").Default()
)

func init() {
	serverStartCommand.Action(repositoryAction(runServer))
}

func runServer(ctx context.Context, rep *repo.Repository) error {
	srv, err := server.New(ctx, rep, getHostName(), getUserName())
	if err != nil {
		return fmt.Errorf("unable to initialize server: %v", err)
	}

	go rep.RefreshPeriodically(ctx, 10*time.Second)

	url := "http://" + *serverAddress
	log.Info().Msgf("starting server on %v", url)
	http.Handle("/api/", srv.APIHandlers())
	return http.ListenAndServe(*serverAddress, nil)
}
