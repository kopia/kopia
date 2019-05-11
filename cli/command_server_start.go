package cli

import (
	"context"
	"net/http"
	"time"

	"github.com/kopia/kopia/internal/server"
	"github.com/kopia/repo"
	"github.com/pkg/errors"
)

var (
	serverAddress = serverCommands.Flag("address", "Server address").Default("127.0.0.1:51515").String()

	serverStartCommand  = serverCommands.Command("start", "Start Kopia server").Default()
	serverStartHTMLPath = serverStartCommand.Flag("html", "Server the provided HTML at the root URL").ExistingDir()
)

func init() {
	serverStartCommand.Action(repositoryAction(runServer))
}

func runServer(ctx context.Context, rep *repo.Repository) error {
	srv, err := server.New(ctx, rep, getHostName(), getUserName())
	if err != nil {
		return errors.Wrap(err, "unable to initialize server")
	}

	go rep.RefreshPeriodically(ctx, 10*time.Second)

	url := "http://" + *serverAddress
	log.Infof("starting server on %v", url)
	http.Handle("/api/", srv.APIHandlers())
	if *serverStartHTMLPath != "" {
		fileServer := http.FileServer(http.Dir(*serverStartHTMLPath))
		http.Handle("/", fileServer)
	}
	return http.ListenAndServe(*serverAddress, nil)
}
