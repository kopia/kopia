package cli

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/server"
	"github.com/kopia/kopia/repo"
)

var (
	serverStartCommand         = serverCommands.Command("start", "Start Kopia server").Default()
	serverStartHTMLPath        = serverStartCommand.Flag("html", "Server the provided HTML at the root URL").ExistingDir()
	serverStartUI              = serverStartCommand.Flag("ui", "Start the server with HTML UI (EXPERIMENTAL)").Bool()
	serverStartRefreshInterval = serverStartCommand.Flag("refresh-interval", "Frequency for refreshing repository status").Default("10s").Duration()

	serverStartRandomPassword = serverStartCommand.Flag("random-password", "Generate random password and print to stderr").Hidden().Bool()
	serverStartAutoShutdown   = serverStartCommand.Flag("auto-shutdown", "Auto shutdown the server if API requests not received within given time").Hidden().Duration()
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

	mux := http.NewServeMux()
	mux.Handle("/api/", srv.APIHandlers())

	if *serverStartHTMLPath != "" {
		fileServer := http.FileServer(http.Dir(*serverStartHTMLPath))
		mux.Handle("/", fileServer)
	} else if *serverStartUI {
		mux.Handle("/", serveIndexFileForKnownUIRoutes(http.FileServer(server.AssetFile())))
	}

	httpServer := &http.Server{Addr: stripProtocol(*serverAddress)}
	srv.OnShutdown = httpServer.Shutdown

	handler := addInterceptors(mux)

	if as := *serverStartAutoShutdown; as > 0 {
		log.Infof("starting a watchdog to stop the server if there's no activity for %v", as)
		handler = startServerWatchdog(handler, as, func() {
			if serr := httpServer.Shutdown(ctx); err != nil {
				log.Warningf("unable to stop the server: %v", serr)
			}
		})
	}

	httpServer.Handler = handler

	err = startServerWithOptionalTLS(httpServer)
	if err == http.ErrServerClosed {
		return nil
	}

	return err
}

func stripProtocol(addr string) string {
	return strings.TrimPrefix(strings.TrimPrefix(addr, "https://"), "http://")
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

func addInterceptors(handler http.Handler) http.Handler {
	if *serverPassword != "" {
		handler = requireAuth{handler, *serverUsername, *serverPassword}
	}

	if *serverStartRandomPassword {
		// generate very long random one-time password
		b := make([]byte, 32)
		io.ReadFull(rand.Reader, b) //nolint:errcheck

		randomPassword := hex.EncodeToString(b)

		// print it to the stderr bypassing any log file so that the user or calling process can connect
		fmt.Fprintln(os.Stderr, "SERVER PASSWORD:", randomPassword)

		handler = requireAuth{handler, *serverUsername, randomPassword}
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
