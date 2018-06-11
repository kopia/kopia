package cli

import (
	"context"
	"net/http"

	"github.com/kopia/kopia/internal/server"
	"github.com/kopia/kopia/repo"
	"github.com/rs/zerolog/log"
)

var (
	serverCommand              = app.Command("server", "Start Kopia server")
	serverCommandListenAddress = serverCommand.Flag("--listen", "Listen address").Default("127.0.0.1:51515").String()
)

func init() {
	serverCommand.Action(repositoryAction(runServer))
}

func runServer(ctx context.Context, rep *repo.Repository) error {
	srv := server.New(rep)
	url := "http://" + *serverCommandListenAddress
	log.Info().Msgf("starting server on %v", url)
	http.Handle("/api/", srv.APIHandlers())
	return http.ListenAndServe(*serverCommandListenAddress, nil)
}
