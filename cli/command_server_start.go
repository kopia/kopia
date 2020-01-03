package cli

import (
	"context"
	"crypto/subtle"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/server"
	"github.com/kopia/kopia/repo"
)

var (
	serverAddress = serverCommands.Flag("address", "Server address").Default("127.0.0.1:51515").String()

	serverStartCommand         = serverCommands.Command("start", "Start Kopia server").Default()
	serverStartHTMLPath        = serverStartCommand.Flag("html", "Server the provided HTML at the root URL").ExistingDir()
	serverStartUI              = serverStartCommand.Flag("ui", "Start the server with HTML UI (EXPERIMENTAL)").Bool()
	serverStartUsername        = serverStartCommand.Flag("server-username", "HTTP server username (basic auth)").Envar("KOPIA_SERVER_USERNAME").Default("kopia").String()
	serverStartPassword        = serverStartCommand.Flag("server-password", "Require HTTP server password (basic auth)").Envar("KOPIA_SERVER_PASSWORD").String()
	serverStartRefreshInterval = serverStartCommand.Flag("refresh-interval", "Frequency for refreshing repository status").Default("10s").Duration()
)

func init() {
	addUserAndHostFlags(serverStartCommand)
	serverStartCommand.Action(repositoryAction(runServer))
}

func runServer(ctx context.Context, rep *repo.Repository) error {
	srv, err := server.New(ctx, rep, getHostName(), getUserName())
	if err != nil {
		return errors.Wrap(err, "unable to initialize server")
	}

	go rep.RefreshPeriodically(ctx, *serverStartRefreshInterval)

	rootURL := "http://" + *serverAddress
	log.Infof("starting server on %v", rootURL)
	http.Handle("/api/", maybeRequireAuth(srv.APIHandlers()))

	if *serverStartHTMLPath != "" {
		fileServer := http.FileServer(http.Dir(*serverStartHTMLPath))
		http.Handle("/", maybeRequireAuth(fileServer))
	} else if *serverStartUI {
		http.Handle("/", maybeRequireAuth(serveIndexFileForKnownUIRoutes(http.FileServer(server.AssetFile()))))
	}

	return http.ListenAndServe(*serverAddress, nil)
}

func isKnownUIRoute(path string) bool {
	return strings.HasPrefix(path, "/snapshots/")
}

func serveIndexFileForKnownUIRoutes(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isKnownUIRoute(r.URL.Path) {
			r2 := new(http.Request)
			*r2 = *r
			r2.URL = new(url.URL)
			*r2.URL = *r.URL
			r2.URL.Path = "/"
			r = r2
		}
		h.ServeHTTP(w, r)
	})
}

func maybeRequireAuth(handler http.Handler) http.Handler {
	if *serverStartPassword != "" {
		return requireAuth{handler, *serverStartUsername, *serverStartPassword}
	}

	return handler
}

type requireAuth struct {
	inner            http.Handler
	expectedUsername string
	expectedPassword string
}

func (a requireAuth) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	user, pass, ok := r.BasicAuth()
	if !ok {
		w.Header().Set("WWW-Authenticate", `Basic realm="Kopia"`)
		http.Error(w, "Missing credentials.\n", http.StatusUnauthorized)

		return
	}

	valid := subtle.ConstantTimeCompare([]byte(user), []byte(a.expectedUsername)) *
		subtle.ConstantTimeCompare([]byte(pass), []byte(a.expectedPassword))

	if valid != 1 {
		w.Header().Set("WWW-Authenticate", `Basic realm="Kopia"`)
		http.Error(w, "Access denied.\n", http.StatusUnauthorized)

		return
	}

	a.inner.ServeHTTP(w, r)
}
