package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"html"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"

	"contrib.go.opencensus.io/exporter/prometheus"
	"github.com/pkg/errors"
	prom "github.com/prometheus/client_golang/prometheus"
	htpasswd "github.com/tg123/go-htpasswd"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/server"
	"github.com/kopia/kopia/repo"
)

var (
	serverStartCommand  = serverCommands.Command("start", "Start Kopia server").Default()
	serverStartHTMLPath = serverStartCommand.Flag("html", "Server the provided HTML at the root URL").ExistingDir()
	serverStartUI       = serverStartCommand.Flag("ui", "Start the server with HTML UI").Default("true").Bool()

	serverStartLegacyRepositoryAPI = serverStartCommand.Flag("legacy-api", "Start the legacy server API").Default("true").Bool()
	serverStartGRPC                = serverStartCommand.Flag("grpc", "Start the GRPC server").Default("true").Bool()

	serverStartRefreshInterval = serverStartCommand.Flag("refresh-interval", "Frequency for refreshing repository status").Default("300s").Duration()
	serverStartInsecure        = serverStartCommand.Flag("insecure", "Allow insecure configurations (do not use in production)").Hidden().Bool()
	serverStartMaxConcurrency  = serverStartCommand.Flag("max-concurrency", "Maximum number of server goroutines").Default("0").Int()

	serverStartRandomPassword = serverStartCommand.Flag("random-password", "Generate random password and print to stderr").Hidden().Bool()
	serverStartHtpasswdFile   = serverStartCommand.Flag("htpasswd-file", "Path to htpasswd file that contains allowed user@hostname entries").Hidden().ExistingFile()
	serverStartAllowRepoUsers = serverStartCommand.Flag("allow-repository-users", "Allow users defined in the repository to connect").Bool()

	serverStartShutdownWhenStdinClosed = serverStartCommand.Flag("shutdown-on-stdin", "Shut down the server when stdin handle has closed.").Hidden().Bool()
)

func init() {
	setupConnectOptions(serverStartCommand)
	serverStartCommand.Action(maybeRepositoryAction(runServer, repositoryAccessMode{
		mustBeConnected:    false,
		disableMaintenance: true, // server closes the repository so maintenance can't run.
	}))
}

func runServer(ctx context.Context, rep repo.Repository) error {
	authn, err := getAuthenticatorFunc(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to initialize authentication")
	}

	srv, err := server.New(ctx, server.Options{
		ConfigFile:      repositoryConfigFileName(),
		ConnectOptions:  connectOptions(),
		RefreshInterval: *serverStartRefreshInterval,
		MaxConcurrency:  *serverStartMaxConcurrency,
		Authenticator:   authn,
		Authorizer:      auth.LegacyAuthorizerForUser,
	})
	if err != nil {
		return errors.Wrap(err, "unable to initialize server")
	}

	if err = maybeAutoUpgradeRepository(ctx, rep); err != nil {
		return errors.Wrap(err, "error upgrading repository")
	}

	if err = srv.SetRepository(ctx, rep); err != nil {
		return errors.Wrap(err, "error connecting to repository")
	}

	mux := http.NewServeMux()

	mux.Handle("/api/", srv.APIHandlers(*serverStartLegacyRepositoryAPI))

	if *serverStartHTMLPath != "" {
		fileServer := serveIndexFileForKnownUIRoutes(http.Dir(*serverStartHTMLPath))
		mux.Handle("/", fileServer)
	} else if *serverStartUI {
		mux.Handle("/", serveIndexFileForKnownUIRoutes(server.AssetFile()))
	}

	httpServer := &http.Server{Addr: stripProtocol(*serverAddress)}
	srv.OnShutdown = httpServer.Shutdown

	onCtrlC(func() {
		log(ctx).Infof("Shutting down...")

		if err = httpServer.Shutdown(ctx); err != nil {
			log(ctx).Warningf("unable to shut down: %v", err)
		}
	})

	// init prometheus after adding interceptors that require credentials, so that this
	// handler can be called without auth
	if err = initPrometheus(mux); err != nil {
		return errors.Wrap(err, "error initializing Prometheus")
	}

	var handler http.Handler = mux

	if *serverStartGRPC {
		handler = srv.GRPCRouterHandler(handler)
	}

	httpServer.Handler = handler

	if *serverStartShutdownWhenStdinClosed {
		log(ctx).Infof("Server will close when stdin is closed...")

		go func() {
			// consume all stdin and close the server when it closes
			ioutil.ReadAll(os.Stdin) //nolint:errcheck
			log(ctx).Infof("Shutting down server...")
			httpServer.Shutdown(ctx) //nolint:errcheck
		}()
	}

	err = startServerWithOptionalTLS(ctx, httpServer)
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return srv.SetRepository(ctx, nil)
}

func initPrometheus(mux *http.ServeMux) error {
	reg := prom.NewRegistry()
	if err := reg.Register(prom.NewProcessCollector(prom.ProcessCollectorOpts{})); err != nil {
		return errors.Wrap(err, "error registering process collector")
	}

	if err := reg.Register(prom.NewGoCollector()); err != nil {
		return errors.Wrap(err, "error registering go collector")
	}

	pe, err := prometheus.NewExporter(prometheus.Options{
		Registry: reg,
	})
	if err != nil {
		return errors.Wrap(err, "unable to initialize prometheus exporter")
	}

	mux.Handle("/metrics", pe)

	return nil
}

func stripProtocol(addr string) string {
	return strings.TrimPrefix(strings.TrimPrefix(addr, "https://"), "http://")
}

func isKnownUIRoute(path string) bool {
	return strings.HasPrefix(path, "/snapshots") ||
		strings.HasPrefix(path, "/policies") ||
		strings.HasPrefix(path, "/tasks") ||
		strings.HasPrefix(path, "/repo")
}

func patchIndexBytes(b []byte) []byte {
	if prefix := os.Getenv("KOPIA_UI_TITLE_PREFIX"); prefix != "" {
		b = bytes.ReplaceAll(b, []byte("<title>"), []byte("<title>"+html.EscapeString(prefix)))
	}

	return b
}

func maybeReadIndexBytes(fs http.FileSystem) []byte {
	rootFile, err := fs.Open("index.html")
	if err != nil {
		return nil
	}

	defer rootFile.Close() //nolint:errcheck

	rd, err := ioutil.ReadAll(rootFile)
	if err != nil {
		return nil
	}

	return rd
}

func serveIndexFileForKnownUIRoutes(fs http.FileSystem) http.Handler {
	h := http.FileServer(fs)

	// read bytes from 'index.html' and patch based on optional environment variables.
	indexBytes := patchIndexBytes(maybeReadIndexBytes(fs))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isKnownUIRoute(r.URL.Path) {
			r2 := new(http.Request)
			*r2 = *r
			r2.URL = new(url.URL)
			*r2.URL = *r.URL
			r2.URL.Path = "/"
			r = r2
		}

		if r.URL.Path == "/" && indexBytes != nil {
			fmt.Println("serving patched index")
			http.ServeContent(w, r, "/", clock.Now(), bytes.NewReader(indexBytes))
			return
		}

		h.ServeHTTP(w, r)
	})
}

func getAuthenticatorFunc(ctx context.Context) (auth.Authenticator, error) {
	switch {
	case *serverStartHtpasswdFile != "":
		f, err := htpasswd.New(*serverStartHtpasswdFile, htpasswd.DefaultSystems, nil)
		if err != nil {
			return nil, errors.Wrap(err, "error initializing htpasswd")
		}

		// f.Match happens to match auth.Authenticator
		return func(ctx context.Context, rep repo.Repository, username, password string) bool {
			return f.Match(username, password)
		}, nil

	case *serverPassword != "":
		return auth.AuthenticateSingleUser(*serverUsername, *serverPassword), nil

	case *serverStartRandomPassword:
		// generate very long random one-time password
		b := make([]byte, 32)
		io.ReadFull(rand.Reader, b) //nolint:errcheck

		randomPassword := hex.EncodeToString(b)

		// print it to the stderr bypassing any log file so that the user or calling process can connect
		fmt.Fprintln(os.Stderr, "SERVER PASSWORD:", randomPassword)

		return auth.AuthenticateSingleUser(*serverUsername, randomPassword), nil

	case *serverStartAllowRepoUsers:
		log(ctx).Noticef(`
Server will allow connections from users whose accounts are stored in the repository.
User accounts can be added using 'kopia user add'.
`)

		return auth.AuthenticateRepositoryUsers(), nil

	default:
		if !*serverStartInsecure {
			return nil, errors.Errorf("no password option specified, refusing to start server. To start non-authenticated server pass --insecure.")
		}

		return nil, nil
	}
}
