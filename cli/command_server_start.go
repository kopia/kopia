package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tg123/go-htpasswd"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/ctxutil"
	"github.com/kopia/kopia/internal/debug"
	"github.com/kopia/kopia/internal/server"
	"github.com/kopia/kopia/repo"
)

const serverRandomPasswordLength = 32

type commandServerStart struct {
	co connectOptions

	serverStartHTMLPath string

	serverStartUI                  bool
	serverStartLegacyRepositoryAPI bool
	serverStartGRPC                bool
	serverStartControlAPI          bool

	serverStartRefreshInterval time.Duration
	serverStartInsecure        bool
	serverStartMaxConcurrency  int

	serverStartWithoutPassword bool
	serverStartRandomPassword  bool
	serverStartHtpasswdFile    string

	randomServerControlPassword bool
	serverControlUsername       string
	serverControlPassword       string

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
	uiPreferencesFile                   string
	asyncRepoConnect                    bool
	persistentLogs                      bool
	debugScheduler                      bool
	minMaintenanceInterval              time.Duration

	shutdownGracePeriod time.Duration

	logServerRequests bool

	disableCSRFTokenChecks bool // disable CSRF token checks - used for development/debugging only

	sf  serverFlags
	svc advancedAppServices
	out textOutput
}

func (c *commandServerStart) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("start", "Start Kopia server")
	cmd.Flag("html", "Server the provided HTML at the root URL").ExistingDirVar(&c.serverStartHTMLPath)
	cmd.Flag("ui", "Start the server with HTML UI").Default("true").BoolVar(&c.serverStartUI)

	cmd.Flag("legacy-api", "Start the legacy server API").Default("true").BoolVar(&c.serverStartLegacyRepositoryAPI)
	cmd.Flag("grpc", "Start the GRPC server").Default("true").BoolVar(&c.serverStartGRPC)
	cmd.Flag("control-api", "Start the control API").Default("true").BoolVar(&c.serverStartControlAPI)

	cmd.Flag("refresh-interval", "Frequency for refreshing repository status").Default("4h").DurationVar(&c.serverStartRefreshInterval)
	cmd.Flag("insecure", "Allow insecure configurations (do not use in production)").Hidden().BoolVar(&c.serverStartInsecure)
	cmd.Flag("max-concurrency", "Maximum number of server goroutines").Default("0").IntVar(&c.serverStartMaxConcurrency)

	cmd.Flag("without-password", "Start the server without a password").Hidden().BoolVar(&c.serverStartWithoutPassword)
	cmd.Flag("random-password", "Generate random password and print to stderr").Hidden().BoolVar(&c.serverStartRandomPassword)
	cmd.Flag("htpasswd-file", "Path to htpasswd file that contains allowed user@hostname entries").Hidden().ExistingFileVar(&c.serverStartHtpasswdFile)

	cmd.Flag("random-server-control-password", "Generate random server control password and print to stderr").Hidden().BoolVar(&c.randomServerControlPassword)
	cmd.Flag("server-control-username", "Server control username").Default("server-control").Envar(svc.EnvName("KOPIA_SERVER_CONTROL_USER")).StringVar(&c.serverControlUsername)
	cmd.Flag("server-control-password", "Server control password").PlaceHolder("PASSWORD").Envar(svc.EnvName("KOPIA_SERVER_CONTROL_PASSWORD")).StringVar(&c.serverControlPassword)

	cmd.Flag("auth-cookie-signing-key", "Force particular auth cookie signing key").Envar(svc.EnvName("KOPIA_AUTH_COOKIE_SIGNING_KEY")).Hidden().StringVar(&c.serverAuthCookieSingingKey)
	cmd.Flag("log-scheduler", "Enable logging of scheduler actions").Hidden().Default("true").BoolVar(&c.debugScheduler)
	cmd.Flag("min-maintenance-interval", "Minimum maintenance interval").Hidden().Default("60s").DurationVar(&c.minMaintenanceInterval)

	cmd.Flag("shutdown-on-stdin", "Shut down the server when stdin handle has closed.").Hidden().BoolVar(&c.serverStartShutdownWhenStdinClosed)

	cmd.Flag("tls-generate-cert", "Generate TLS certificate").Hidden().BoolVar(&c.serverStartTLSGenerateCert)
	cmd.Flag("tls-cert-file", "TLS certificate PEM").StringVar(&c.serverStartTLSCertFile)
	cmd.Flag("tls-key-file", "TLS key PEM file").StringVar(&c.serverStartTLSKeyFile)
	cmd.Flag("tls-generate-rsa-key-size", "TLS RSA Key size (bits)").Hidden().Default("4096").IntVar(&c.serverStartTLSGenerateRSAKeySize)
	cmd.Flag("tls-generate-cert-valid-days", "How long should the TLS certificate be valid").Default("3650").Hidden().IntVar(&c.serverStartTLSGenerateCertValidDays)
	cmd.Flag("tls-generate-cert-name", "Host names/IP addresses to generate TLS certificate for").Default("127.0.0.1").Hidden().StringsVar(&c.serverStartTLSGenerateCertNames)
	cmd.Flag("tls-print-server-cert", "Print server certificate").Hidden().BoolVar(&c.serverStartTLSPrintFullServerCert)

	cmd.Flag("async-repo-connect", "Connect to repository asynchronously").Hidden().BoolVar(&c.asyncRepoConnect)
	cmd.Flag("persistent-logs", "Persist logs in a file").Default("true").BoolVar(&c.persistentLogs)
	cmd.Flag("ui-title-prefix", "UI title prefix").Hidden().Envar(svc.EnvName("KOPIA_UI_TITLE_PREFIX")).StringVar(&c.uiTitlePrefix)
	cmd.Flag("ui-preferences-file", "Path to JSON file storing UI preferences").StringVar(&c.uiPreferencesFile)

	cmd.Flag("log-server-requests", "Log server requests").Hidden().BoolVar(&c.logServerRequests)
	cmd.Flag("disable-csrf-token-checks", "Disable CSRF token").Hidden().BoolVar(&c.disableCSRFTokenChecks)

	cmd.Flag("shutdown-grace-period", "Grace period for shutting down the server").Default("5s").DurationVar(&c.shutdownGracePeriod)

	c.sf.setup(svc, cmd)
	c.co.setup(svc, cmd)
	c.svc = svc
	c.out.setup(svc)

	cmd.Action(svc.baseActionWithContext(c.run))
}

func (c *commandServerStart) serverStartOptions(ctx context.Context) (*server.Options, error) {
	authn, err := c.getAuthenticator(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize authentication")
	}

	uiPreferencesFile := c.uiPreferencesFile
	if uiPreferencesFile == "" {
		uiPreferencesFile = filepath.Join(filepath.Dir(c.svc.repositoryConfigFileName()), "ui-preferences.json")
	}

	return &server.Options{
		ConfigFile:           c.svc.repositoryConfigFileName(),
		ConnectOptions:       c.co.toRepoConnectOptions(),
		RefreshInterval:      c.serverStartRefreshInterval,
		MaxConcurrency:       c.serverStartMaxConcurrency,
		Authenticator:        authn,
		Authorizer:           auth.DefaultAuthorizer(),
		AuthCookieSigningKey: c.serverAuthCookieSingingKey,
		UIUser:               c.sf.serverUsername,
		ServerControlUser:    c.serverControlUsername,
		LogRequests:          c.logServerRequests,
		PasswordPersist:      c.svc.passwordPersistenceStrategy(),
		UIPreferencesFile:    uiPreferencesFile,
		UITitlePrefix:        c.uiTitlePrefix,
		PersistentLogs:       c.persistentLogs,

		DebugScheduler:         c.debugScheduler,
		MinMaintenanceInterval: c.minMaintenanceInterval,
		DisableCSRFTokenChecks: c.disableCSRFTokenChecks,
	}, nil
}

func (c *commandServerStart) initRepositoryPossiblyAsync(ctx context.Context, srv *server.Server) error {
	initialize := func(ctx context.Context) (repo.Repository, error) {
		//nolint:wrapcheck
		return c.svc.openRepository(ctx, false)
	}

	if c.asyncRepoConnect {
		// retry initialization indefinitely
		initialize = server.RetryInitRepository(initialize)
	}

	if _, err := srv.InitRepositoryAsync(ctx, "Open", initialize, !c.asyncRepoConnect); err != nil {
		return errors.Wrap(err, "unable to initialize repository")
	}

	return nil
}

func (c *commandServerStart) run(ctx context.Context) error {
	opts, err := c.serverStartOptions(ctx)
	if err != nil {
		return err
	}

	srv, err := server.New(ctx, opts)
	if err != nil {
		return errors.Wrap(err, "unable to initialize server")
	}

	if err = c.initRepositoryPossiblyAsync(ctx, srv); err != nil {
		return errors.Wrap(err, "unable to initialize repository")
	}

	httpServer := &http.Server{
		ReadHeaderTimeout: 15 * time.Second, //nolint:gomnd
		Addr:              stripProtocol(c.sf.serverAddress),
		BaseContext: func(l net.Listener) context.Context {
			return ctx
		},
	}

	srv.OnShutdown = func(ctx context.Context) error {
		ctx2, cancel := context.WithTimeout(ctx, c.shutdownGracePeriod)
		defer cancel()

		// wait for all connections to finish for up to 5 seconds
		log(ctx2).Debugf("attempting graceful shutdown for %v", c.shutdownGracePeriod)

		debug.StopProfileBuffers(ctx)

		if serr := httpServer.Shutdown(ctx2); serr != nil {
			// graceful shutdown unsuccessful, force close
			log(ctx2).Debugf("unable to shut down gracefully - closing: %v", serr)
			return errors.Wrap(httpServer.Close(), "close")
		}

		log(ctx2).Debugf("graceful shutdown succeeded")

		return nil
	}

	c.svc.onTerminate(func() {
		log(ctx).Infof("Shutting down...")
		shutdownServer(ctx, httpServer)
	})

	c.svc.onRepositoryFatalError(func(_ error) {
		if serr := httpServer.Shutdown(ctx); serr != nil {
			log(ctx).Debugf("unable to shut down: %v", serr)
		}
	})

	c.svc.onSigDump(func() {
		debug.StopProfileBuffers(ctx)
		debug.StartProfileBuffers(ctx)
	})

	c.svc.onRepositoryFatalError(func(error) { shutdownServer(ctx, httpServer) })

	m := mux.NewRouter()

	c.setupHandlers(srv, m)

	// init prometheus after adding interceptors that require credentials, so that this
	// handler can be called without auth
	initPrometheus(m)

	var handler http.Handler = m

	if c.serverStartGRPC {
		handler = srv.GRPCRouterHandler(handler)
	}

	httpServer.Handler = handler

	if c.serverStartShutdownWhenStdinClosed {
		log(ctx).Infof("Server will close when stdin is closed...")

		ctxutil.GoDetached(ctx, func(ctx context.Context) {
			// consume all stdin and close the server when it closes
			io.ReadAll(os.Stdin) //nolint:errcheck
			log(ctx).Infof("Shutting down server...")
			httpServer.Shutdown(ctx) //nolint:errcheck
		})
	}

	onExternalConfigReloadRequest(srv.Refresh)

	err = c.startServerWithOptionalTLS(ctx, httpServer)
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return errors.Wrap(srv.SetRepository(ctx, nil), "error setting active repository")
}

// shutdownServer shutdown http server and close the repository.
func shutdownServer(ctx context.Context, httpServer *http.Server) {
	log(ctx).Infof("Shutting down...")

	debug.StopProfileBuffers(ctx)

	if serr := httpServer.Shutdown(ctx); serr != nil {
		log(ctx).Debugf("unable to shut down http server: %v", serr)
	}
}

func (c *commandServerStart) setupHandlers(srv *server.Server, m *mux.Router) {
	if c.serverStartLegacyRepositoryAPI {
		srv.SetupRepositoryAPIHandlers(m)
	}

	if c.serverStartControlAPI {
		srv.SetupControlAPIHandlers(m)
	}

	if c.serverStartUI {
		srv.SetupHTMLUIAPIHandlers(m)

		if c.serverStartHTMLPath != "" {
			srv.ServeStaticFiles(m, http.Dir(c.serverStartHTMLPath))
		} else {
			srv.ServeStaticFiles(m, server.AssetFile())
		}
	}
}

func initPrometheus(m *mux.Router) {
	m.Handle("/metrics", promhttp.Handler())
}

func stripProtocol(addr string) string {
	return strings.TrimPrefix(strings.TrimPrefix(addr, "https://"), "http://")
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
			return nil, errors.Errorf("--without-password specified without --insecure, refusing to start server")
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

	// handle server control password
	switch {
	case c.serverControlPassword != "":
		authenticators = append(authenticators, auth.AuthenticateSingleUser(c.serverControlUsername, c.serverControlPassword))

	case c.randomServerControlPassword:
		// generate very long random one-time password
		b := make([]byte, serverRandomPasswordLength)
		io.ReadFull(rand.Reader, b) //nolint:errcheck

		randomPassword := hex.EncodeToString(b)

		// print it to the stderr bypassing any log file so that the user or calling process can connect
		fmt.Fprintln(c.out.stderr(), "SERVER CONTROL PASSWORD:", randomPassword)

		authenticators = append(authenticators, auth.AuthenticateSingleUser(c.serverControlUsername, randomPassword))
	}

	log(ctx).Infof(`
Server will allow connections from users whose accounts are stored in the repository.
User accounts can be added using 'kopia server user add'.
`)

	// handle user accounts stored in the repository
	authenticators = append(authenticators, auth.AuthenticateRepositoryUsers())

	return auth.CombineAuthenticators(authenticators...), nil
}
