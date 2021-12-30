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

	"contrib.go.opencensus.io/exporter/prometheus"
	"github.com/pkg/errors"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	htpasswd "github.com/tg123/go-htpasswd"

	"github.com/kopia/kopia/internal/auth"
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
	serverPromptBasicAuth      bool

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

	uiSingleUseAuthTokenTTL time.Duration
	uiSessionCookieTTL      time.Duration

	logServerRequests bool

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

	cmd.Flag("random-server-control-password", "Generate random server control password and print to stderr").Hidden().BoolVar(&c.randomServerControlPassword)
	cmd.Flag("server-control-username", "Server control username").Default("server-control").Envar("KOPIA_SERVER_CONTROL_USER").StringVar(&c.serverControlUsername)
	cmd.Flag("server-control-password", "Server control password").PlaceHolder("PASSWORD").Envar("KOPIA_SERVER_CONTROL_PASSWORD").StringVar(&c.serverControlPassword)

	cmd.Flag("prompt-basic-auth", "Prompt for basic authentication if not provided").Default("true").Hidden().BoolVar(&c.serverPromptBasicAuth)
	cmd.Flag("auth-cookie-signing-key", "Force particular auth cookie signing key").Envar("KOPIA_AUTH_COOKIE_SIGNING_KEY").Hidden().StringVar(&c.serverAuthCookieSingingKey)
	cmd.Flag("shutdown-on-stdin", "Shut down the server when stdin handle has closed.").Hidden().BoolVar(&c.serverStartShutdownWhenStdinClosed)
	cmd.Flag("tls-generate-cert", "Generate TLS certificate").Hidden().BoolVar(&c.serverStartTLSGenerateCert)
	cmd.Flag("tls-cert-file", "TLS certificate PEM").StringVar(&c.serverStartTLSCertFile)
	cmd.Flag("tls-key-file", "TLS key PEM file").StringVar(&c.serverStartTLSKeyFile)
	cmd.Flag("tls-generate-rsa-key-size", "TLS RSA Key size (bits)").Hidden().Default("4096").IntVar(&c.serverStartTLSGenerateRSAKeySize)
	cmd.Flag("tls-generate-cert-valid-days", "How long should the TLS certificate be valid").Default("3650").Hidden().IntVar(&c.serverStartTLSGenerateCertValidDays)
	cmd.Flag("tls-generate-cert-name", "Host names/IP addresses to generate TLS certificate for").Default("127.0.0.1").Hidden().StringsVar(&c.serverStartTLSGenerateCertNames)
	cmd.Flag("tls-print-server-cert", "Print server certificate").Hidden().BoolVar(&c.serverStartTLSPrintFullServerCert)
	cmd.Flag("ui-auth-token-ttl", "Time to live for single-use UI auth tokens").Hidden().DurationVar(&c.uiSingleUseAuthTokenTTL)
	cmd.Flag("ui-session-cookie-ttl", "Time to live for UI session cookies (auto-extended on each request)").Default("600s").Hidden().DurationVar(&c.uiSessionCookieTTL)

	cmd.Flag("ui-title-prefix", "UI title prefix").Hidden().Envar("KOPIA_UI_TITLE_PREFIX").StringVar(&c.uiTitlePrefix)
	cmd.Flag("ui-preferences-file", "Path to JSON file storing UI preferences").StringVar(&c.uiPreferencesFile)

	cmd.Flag("log-server-requests", "Log server requests").Hidden().BoolVar(&c.logServerRequests)

	c.sf.setup(cmd)
	c.co.setup(cmd)
	c.svc = svc
	c.out.setup(svc)

	cmd.Action(svc.maybeRepositoryAction(c.run, repositoryAccessMode{
		mustBeConnected:    false,
		disableMaintenance: true, // server closes the repository so maintenance can't run.
	}))
}

// nolint:funlen
func (c *commandServerStart) run(ctx context.Context, rep repo.Repository) error {
	authn, err := c.getAuthenticator(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to initialize authentication")
	}

	uiPreferencesFile := c.uiPreferencesFile
	if uiPreferencesFile == "" {
		uiPreferencesFile = filepath.Join(filepath.Dir(c.svc.repositoryConfigFileName()), "ui-preferences.json")
	}

	srv, err := server.New(ctx, &server.Options{
		ConfigFile:              c.svc.repositoryConfigFileName(),
		ConnectOptions:          c.co.toRepoConnectOptions(),
		RefreshInterval:         c.serverStartRefreshInterval,
		MaxConcurrency:          c.serverStartMaxConcurrency,
		Authenticator:           authn,
		Authorizer:              auth.DefaultAuthorizer(),
		AuthCookieSigningKey:    c.serverAuthCookieSingingKey,
		UIUser:                  c.sf.serverUsername,
		ServerControlUser:       c.serverControlUsername,
		LogRequests:             c.logServerRequests,
		PasswordPersist:         c.svc.passwordPersistenceStrategy(),
		UIPreferencesFile:       uiPreferencesFile,
		UITitlePrefix:           c.uiTitlePrefix,
		PromptForBasicAuth:      c.serverPromptBasicAuth,
		SingleUseUIAuthTokenTTL: c.uiSingleUseAuthTokenTTL,
		UISessionCookieTTL:      c.uiSessionCookieTTL,
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
		fileServer := srv.ServeStaticFiles(http.Dir(c.serverStartHTMLPath))
		mux.Handle("/", fileServer)
	} else if c.serverStartUI {
		mux.Handle("/", srv.ServeStaticFiles(server.AssetFile()))
	}

	httpServer := &http.Server{
		Addr: stripProtocol(c.sf.serverAddress),
		BaseContext: func(l net.Listener) context.Context {
			return ctx
		},
	}

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
			io.ReadAll(os.Stdin) //nolint:errcheck
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
	if err := reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return errors.Wrap(err, "error registering process collector")
	}

	if err := reg.Register(collectors.NewGoCollector()); err != nil {
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
