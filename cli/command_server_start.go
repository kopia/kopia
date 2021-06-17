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
	"time"

	"contrib.go.opencensus.io/exporter/prometheus"
	"github.com/pkg/errors"
	prom "github.com/prometheus/client_golang/prometheus"
	htpasswd "github.com/tg123/go-htpasswd"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/server"
	"github.com/kopia/kopia/repo"
)

const serverRandomPasswordLength = 32

type commandServerStart struct {
	co connectOptions

	serverStartHTMLPath string
	serverStartUI       bool

	serverStartLegacyRepositoryAPI bool
	serverStartGRPC                bool

	serverStartRefreshInterval time.Duration
	serverStartInsecure        bool
	serverStartMaxConcurrency  int

	serverStartWithoutPassword bool
	serverStartRandomPassword  bool
	serverStartHtpasswdFile    string

	serverAuthCookieSingingKey string

	serverStartShutdownWhenStdinClosed bool

	serverStartTLSGenerateCert          bool
	serverStartTLSCertFile              string
	serverStartTLSKeyFile               string
	serverStartTLSGenerateRSAKeySize    int
	serverStartTLSGenerateCertValidDays int
	serverStartTLSGenerateCertNames     []string
	serverStartTLSPrintFullServerCert   bool
	uiTitlePrefix                       string

	sf  serverFlags
	svc advancedAppServices
	out textOutput
}

func (c *commandServerStart) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("start", "Start Kopia server").Default()
	cmd.Flag("html", "Server the provided HTML at the root URL").ExistingDirVar(&c.serverStartHTMLPath)
	cmd.Flag("ui", "Start the server with HTML UI").Default("true").BoolVar(&c.serverStartUI)

	cmd.Flag("legacy-api", "Start the legacy server API").Default("true").BoolVar(&c.serverStartLegacyRepositoryAPI)
	cmd.Flag("grpc", "Start the GRPC server").Default("true").BoolVar(&c.serverStartGRPC)

	cmd.Flag("refresh-interval", "Frequency for refreshing repository status").Default("300s").DurationVar(&c.serverStartRefreshInterval)
	cmd.Flag("insecure", "Allow insecure configurations (do not use in production)").Hidden().BoolVar(&c.serverStartInsecure)
	cmd.Flag("max-concurrency", "Maximum number of server goroutines").Default("0").IntVar(&c.serverStartMaxConcurrency)

	cmd.Flag("without-password", "Start the server without a password").Hidden().BoolVar(&c.serverStartWithoutPassword)
	cmd.Flag("random-password", "Generate random password and print to stderr").Hidden().BoolVar(&c.serverStartRandomPassword)
	cmd.Flag("htpasswd-file", "Path to htpasswd file that contains allowed user@hostname entries").Hidden().ExistingFileVar(&c.serverStartHtpasswdFile)

	cmd.Flag("auth-cookie-signing-key", "Force particular auth cookie signing key").Envar("KOPIA_AUTH_COOKIE_SIGNING_KEY").Hidden().StringVar(&c.serverAuthCookieSingingKey)

	cmd.Flag("shutdown-on-stdin", "Shut down the server when stdin handle has closed.").Hidden().BoolVar(&c.serverStartShutdownWhenStdinClosed)

	cmd.Flag("tls-generate-cert", "Generate TLS certificate").Hidden().BoolVar(&c.serverStartTLSGenerateCert)
	cmd.Flag("tls-cert-file", "TLS certificate PEM").StringVar(&c.serverStartTLSCertFile)
	cmd.Flag("tls-key-file", "TLS key PEM file").StringVar(&c.serverStartTLSKeyFile)
	cmd.Flag("tls-generate-rsa-key-size", "TLS RSA Key size (bits)").Hidden().Default("4096").IntVar(&c.serverStartTLSGenerateRSAKeySize)
	cmd.Flag("tls-generate-cert-valid-days", "How long should the TLS certificate be valid").Default("3650").Hidden().IntVar(&c.serverStartTLSGenerateCertValidDays)
	cmd.Flag("tls-generate-cert-name", "Host names/IP addresses to generate TLS certificate for").Default("127.0.0.1").Hidden().StringsVar(&c.serverStartTLSGenerateCertNames)
	cmd.Flag("tls-print-server-cert", "Print server certificate").Hidden().BoolVar(&c.serverStartTLSPrintFullServerCert)

	cmd.Flag("ui-title-prefix", "UI title prefix").Hidden().Envar("KOPIA_UI_TITLE_PREFIX").StringVar(&c.uiTitlePrefix)

	c.sf.setup(cmd)
	c.co.setup(cmd)
	c.svc = svc
	c.out.setup(svc)

	cmd.Action(svc.maybeRepositoryAction(c.run, repositoryAccessMode{
		mustBeConnected:    false,
		disableMaintenance: true, // server closes the repository so maintenance can't run.
	}))
}

func (c *commandServerStart) run(ctx context.Context, rep repo.Repository) error {
	authn, err := c.getAuthenticator(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to initialize authentication")
	}

	srv, err := server.New(ctx, server.Options{
		ConfigFile:           c.svc.repositoryConfigFileName(),
		ConnectOptions:       c.co.toRepoConnectOptions(),
		RefreshInterval:      c.serverStartRefreshInterval,
		MaxConcurrency:       c.serverStartMaxConcurrency,
		Authenticator:        authn,
		Authorizer:           auth.DefaultAuthorizer(),
		AuthCookieSigningKey: c.serverAuthCookieSingingKey,
		UIUser:               c.sf.serverUsername,
		PasswordPersist:      c.svc.passwordPersistenceStrategy(),
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

	mux.Handle("/api/", srv.APIHandlers(c.serverStartLegacyRepositoryAPI))

	if c.serverStartHTMLPath != "" {
		fileServer := srv.RequireUIUserAuth(c.serveIndexFileForKnownUIRoutes(http.Dir(c.serverStartHTMLPath)))
		mux.Handle("/", fileServer)
	} else if c.serverStartUI {
		mux.Handle("/", srv.RequireUIUserAuth(c.serveIndexFileForKnownUIRoutes(server.AssetFile())))
	}

	httpServer := &http.Server{Addr: stripProtocol(c.sf.serverAddress)}
	srv.OnShutdown = httpServer.Shutdown

	onCtrlC(func() {
		log(ctx).Infof("Shutting down...")

		if err = httpServer.Shutdown(ctx); err != nil {
			log(ctx).Debugf("unable to shut down: %v", err)
		}
	})

	// init prometheus after adding interceptors that require credentials, so that this
	// handler can be called without auth
	if err = initPrometheus(mux); err != nil {
		return errors.Wrap(err, "error initializing Prometheus")
	}

	var handler http.Handler = mux

	if c.serverStartGRPC {
		handler = srv.GRPCRouterHandler(handler)
	}

	httpServer.Handler = handler

	if c.serverStartShutdownWhenStdinClosed {
		log(ctx).Infof("Server will close when stdin is closed...")

		go func() {
			// consume all stdin and close the server when it closes
			ioutil.ReadAll(os.Stdin) //nolint:errcheck
			log(ctx).Infof("Shutting down server...")
			httpServer.Shutdown(ctx) //nolint:errcheck
		}()
	}

	onExternalConfigReloadRequest(func() {
		if rerr := srv.Refresh(ctx); rerr != nil {
			log(ctx).Errorf("refresh failed: %v", rerr)
		}
	})

	err = c.startServerWithOptionalTLS(ctx, httpServer)
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return errors.Wrap(srv.SetRepository(ctx, nil), "error setting active repository")
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

func (c *commandServerStart) isKnownUIRoute(path string) bool {
	return strings.HasPrefix(path, "/snapshots") ||
		strings.HasPrefix(path, "/policies") ||
		strings.HasPrefix(path, "/tasks") ||
		strings.HasPrefix(path, "/repo")
}

func (c *commandServerStart) patchIndexBytes(b []byte) []byte {
	if c.uiTitlePrefix != "" {
		b = bytes.ReplaceAll(b, []byte("<title>"), []byte("<title>"+html.EscapeString(c.uiTitlePrefix)))
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

func (c *commandServerStart) serveIndexFileForKnownUIRoutes(fs http.FileSystem) http.Handler {
	h := http.FileServer(fs)

	// read bytes from 'index.html' and patch based on optional environment variables.
	indexBytes := c.patchIndexBytes(maybeReadIndexBytes(fs))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c.isKnownUIRoute(r.URL.Path) {
			r2 := new(http.Request)
			*r2 = *r
			r2.URL = new(url.URL)
			*r2.URL = *r.URL
			r2.URL.Path = "/"
			r = r2
		}

		if r.URL.Path == "/" && indexBytes != nil {
			http.ServeContent(w, r, "/", clock.Now(), bytes.NewReader(indexBytes))
			return
		}

		h.ServeHTTP(w, r)
	})
}

func (c *commandServerStart) getAuthenticator(ctx context.Context) (auth.Authenticator, error) {
	var authenticators []auth.Authenticator

	// handle passwords (UI and remote) from htpasswd file.
	if c.serverStartHtpasswdFile != "" {
		f, err := htpasswd.New(c.serverStartHtpasswdFile, htpasswd.DefaultSystems, nil)
		if err != nil {
			return nil, errors.Wrap(err, "error initializing htpasswd")
		}

		authenticators = append(authenticators, auth.AuthenticateHtpasswdFile(f))
	}

	// handle UI password (--without-password, --password or --random-password)
	switch {
	case c.serverStartWithoutPassword:
		if !c.serverStartInsecure {
			return nil, errors.Errorf("--without-password specified without --insecure, refusing to start server.")
		}

		return nil, nil

	case c.sf.serverPassword != "":
		authenticators = append(authenticators, auth.AuthenticateSingleUser(c.sf.serverUsername, c.sf.serverPassword))

	case c.serverStartRandomPassword:
		// generate very long random one-time password
		b := make([]byte, serverRandomPasswordLength)
		io.ReadFull(rand.Reader, b) //nolint:errcheck

		randomPassword := hex.EncodeToString(b)

		// print it to the stderr bypassing any log file so that the user or calling process can connect
		fmt.Fprintln(c.out.stderr(), "SERVER PASSWORD:", randomPassword)

		authenticators = append(authenticators, auth.AuthenticateSingleUser(c.sf.serverUsername, randomPassword))
	}

	log(ctx).Infof(`
Server will allow connections from users whose accounts are stored in the repository.
User accounts can be added using 'kopia server user add'.
`)

	// handle user accounts stored in the repository
	authenticators = append(authenticators, auth.AuthenticateRepositoryUsers())

	return auth.CombineAuthenticators(authenticators...), nil
}
