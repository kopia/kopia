// Package cli implements command-line commands for the Kopia.
package cli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/fatih/color"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/passwordpersist"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

var log = logging.GetContextLoggerFunc("kopia/cli")

var (
	defaultColor = color.New()
	warningColor = color.New(color.FgYellow)
	errorColor   = color.New(color.FgHiRed)
)

type textOutput struct {
	svc appServices
}

func (o *textOutput) setup(svc appServices) {
	s := string(debug.Stack())
	if strings.Contains(s, "cliProgress") {
		fmt.Printf("setting up %s with %v\n", debug.Stack(), svc.stdout())
	}

	o.svc = svc
}

func (o *textOutput) stdout() io.Writer {
	if o.svc == nil {
		return os.Stdout
	}

	return o.svc.stdout()
}

func (o *textOutput) stderr() io.Writer {
	if o.svc == nil {
		return os.Stderr
	}

	return o.svc.stderr()
}

func (o *textOutput) printStdout(msg string, args ...interface{}) {
	fmt.Fprintf(o.stdout(), msg, args...)
}

func (o *textOutput) printStderr(msg string, args ...interface{}) {
	fmt.Fprintf(o.stderr(), msg, args...)
}

// appServices are the methods of *App that command handles are allowed to call.
type appServices interface {
	noRepositoryAction(act func(ctx context.Context) error) func(ctx *kingpin.ParseContext) error
	serverAction(sf *serverClientFlags, act func(ctx context.Context, cli *apiclient.KopiaAPIClient) error) func(ctx *kingpin.ParseContext) error
	directRepositoryWriteAction(act func(ctx context.Context, rep repo.DirectRepositoryWriter) error) func(ctx *kingpin.ParseContext) error
	directRepositoryReadAction(act func(ctx context.Context, rep repo.DirectRepository) error) func(ctx *kingpin.ParseContext) error
	repositoryReaderAction(act func(ctx context.Context, rep repo.Repository) error) func(ctx *kingpin.ParseContext) error
	repositoryWriterAction(act func(ctx context.Context, rep repo.RepositoryWriter) error) func(ctx *kingpin.ParseContext) error
	maybeRepositoryAction(act func(ctx context.Context, rep repo.Repository) error, mode repositoryAccessMode) func(ctx *kingpin.ParseContext) error

	advancedCommand(ctx context.Context)
	repositoryConfigFileName() string
	getProgress() *cliProgress

	stdout() io.Writer
	stderr() io.Writer
}

type advancedAppServices interface {
	appServices
	storageProviderServices

	runConnectCommandWithStorage(ctx context.Context, co *connectOptions, st blob.Storage) error
	runConnectCommandWithStorageAndPassword(ctx context.Context, co *connectOptions, st blob.Storage, password string) error
	openRepository(ctx context.Context, required bool) (repo.Repository, error)

	maybeInitializeUpdateCheck(ctx context.Context, co *connectOptions)
	removeUpdateState()
	passwordPersistenceStrategy() passwordpersist.Strategy
	getPasswordFromFlags(ctx context.Context, isNew, allowPersistent bool) (string, error)
	optionsFromFlags(ctx context.Context) *repo.Options

	rootContext() context.Context
}

// App contains per-invocation flags and state of Kopia CLI.
type App struct {
	// global flags
	enableAutomaticMaintenance    bool
	mt                            memoryTracker
	progress                      *cliProgress
	initialUpdateCheckDelay       time.Duration
	updateCheckInterval           time.Duration
	updateAvailableNotifyInterval time.Duration
	password                      string
	configPath                    string
	traceStorage                  bool
	metricsListenAddr             string
	keyRingEnabled                bool
	persistCredentials            bool
	AdvancedCommands              string

	// subcommands
	blob        commandBlob
	benchmark   commandBenchmark
	cache       commandCache
	content     commandContent
	diff        commandDiff
	index       commandIndex
	list        commandList
	server      commandServer
	session     commandSession
	policy      commandPolicy
	restore     commandRestore
	show        commandShow
	snapshot    commandSnapshot
	manifest    commandManifest
	mount       commandMount
	maintenance commandMaintenance
	repository  commandRepository

	// testability hooks
	osExit       func(int) // allows replacing os.Exit() with custom code
	stdoutWriter io.Writer
	stderrWriter io.Writer
	rootctx      context.Context
}

func (c *App) getProgress() *cliProgress {
	return c.progress
}

func (c *App) stdout() io.Writer {
	return c.stdoutWriter
}

func (c *App) stderr() io.Writer {
	return c.stderrWriter
}

func (c *App) passwordPersistenceStrategy() passwordpersist.Strategy {
	if !c.persistCredentials {
		return passwordpersist.None
	}

	if c.keyRingEnabled {
		return passwordpersist.Multiple{
			passwordpersist.Keyring,
			passwordpersist.File,
		}
	}

	return passwordpersist.File
}

func (c *App) setup(app *kingpin.Application) {
	_ = app.Flag("help-full", "Show help for all commands, including hidden").Action(func(pc *kingpin.ParseContext) error {
		_ = app.UsageForContextWithTemplate(pc, 0, kingpin.DefaultUsageTemplate)
		os.Exit(0)
		return nil
	}).Bool()

	app.Flag("auto-maintenance", "Automatic maintenance").Default("true").Hidden().BoolVar(&c.enableAutomaticMaintenance)

	// hidden flags to control auto-update behavior.
	app.Flag("initial-update-check-delay", "Initial delay before first time update check").Default("24h").Hidden().Envar("KOPIA_INITIAL_UPDATE_CHECK_DELAY").DurationVar(&c.initialUpdateCheckDelay)
	app.Flag("update-check-interval", "Interval between update checks").Default("168h").Hidden().Envar("KOPIA_UPDATE_CHECK_INTERVAL").DurationVar(&c.updateCheckInterval)
	app.Flag("update-available-notify-interval", "Interval between update notifications").Default("1h").Hidden().Envar("KOPIA_UPDATE_NOTIFY_INTERVAL").DurationVar(&c.updateAvailableNotifyInterval)
	app.Flag("config-file", "Specify the config file to use.").Default(defaultConfigFileName()).Envar("KOPIA_CONFIG_PATH").StringVar(&c.configPath)
	app.Flag("trace-storage", "Enables tracing of storage operations.").Default("true").Hidden().BoolVar(&c.traceStorage)
	app.Flag("metrics-listen-addr", "Expose Prometheus metrics on a given host:port").Hidden().StringVar(&c.metricsListenAddr)
	app.Flag("timezone", "Format time according to specified time zone (local, utc, original or time zone name)").Hidden().StringVar(&timeZone)
	app.Flag("password", "Repository password.").Envar("KOPIA_PASSWORD").Short('p').StringVar(&c.password)
	app.Flag("persist-credentials", "Persist credentials").Default("true").Envar("KOPIA_PERSIST_CREDENTIALS_ON_CONNECT").BoolVar(&c.persistCredentials)
	app.Flag("advanced-commands", "Enable advanced (and potentially dangerous) commands.").Hidden().Envar("KOPIA_ADVANCED_COMMANDS").StringVar(&c.AdvancedCommands)

	c.setupOSSpecificKeychainFlags(app)

	_ = app.Flag("caching", "Enables caching of objects (disable with --no-caching)").Default("true").Hidden().Action(
		deprecatedFlag(c.stderrWriter, "The '--caching' flag is deprecated and has no effect, use 'kopia cache set' instead."),
	).Bool()

	_ = app.Flag("list-caching", "Enables caching of list results (disable with --no-list-caching)").Default("true").Hidden().Action(
		deprecatedFlag(c.stderrWriter, "The '--list-caching' flag is deprecated and has no effect, use 'kopia cache set' instead."),
	).Bool()

	c.mt.setup(app)
	c.progress.setup(c, app)

	c.blob.setup(c, app)
	c.benchmark.setup(c, app)
	c.cache.setup(c, app)
	c.content.setup(c, app)
	c.diff.setup(c, app)
	c.index.setup(c, app)
	c.list.setup(c, app)
	c.server.setup(c, app)
	c.session.setup(c, app)
	c.restore.setup(c, app)
	c.show.setup(c, app)
	c.snapshot.setup(c, app)
	c.manifest.setup(c, app)
	c.policy.setup(c, app)
	c.mount.setup(c, app)
	c.maintenance.setup(c, app)
	c.repository.setup(c, app)
}

// commandParent is implemented by app and commands that can have sub-commands.
type commandParent interface {
	Command(name, help string) *kingpin.CmdClause
}

// NewApp creates a new instance of App.
func NewApp() *App {
	return &App{
		progress: &cliProgress{},

		// testability hooks
		osExit:       os.Exit,
		stdoutWriter: os.Stdout,
		stderrWriter: os.Stderr,
		rootctx:      context.Background(),
	}
}

// Attach attaches the CLI parser to the application.
func (c *App) Attach(app *kingpin.Application) {
	c.setup(app)
}

var safetyByName = map[string]maintenance.SafetyParameters{
	"none": maintenance.SafetyNone,
	"full": maintenance.SafetyFull,
}

// safetyFlagVar defines c --safety=none|full flag that sets the SafetyParameters.
func safetyFlagVar(cmd *kingpin.CmdClause, result *maintenance.SafetyParameters) {
	var str string

	*result = maintenance.SafetyFull

	cmd.Flag("safety", "Safety level").Default("full").PreAction(func(pc *kingpin.ParseContext) error {
		r, ok := safetyByName[str]
		if !ok {
			return errors.Errorf("unhandled safety level")
		}

		*result = r

		return nil
	}).EnumVar(&str, "full", "none")
}

func (c *App) noRepositoryAction(act func(ctx context.Context) error) func(ctx *kingpin.ParseContext) error {
	return func(_ *kingpin.ParseContext) error {
		return act(c.rootContext())
	}
}

func (c *App) serverAction(sf *serverClientFlags, act func(ctx context.Context, cli *apiclient.KopiaAPIClient) error) func(ctx *kingpin.ParseContext) error {
	return func(_ *kingpin.ParseContext) error {
		opts, err := sf.serverAPIClientOptions()
		if err != nil {
			return errors.Wrap(err, "unable to create API client options")
		}

		apiClient, err := apiclient.NewKopiaAPIClient(opts)
		if err != nil {
			return errors.Wrap(err, "unable to create API client")
		}

		return act(c.rootContext(), apiClient)
	}
}

func assertDirectRepository(act func(ctx context.Context, rep repo.DirectRepository) error) func(ctx context.Context, rep repo.Repository) error {
	return func(ctx context.Context, rep repo.Repository) error {
		if rep == nil {
			return act(ctx, nil)
		}

		// right now this assertion never fails,
		// but will fail in the future when we have remote repository implementation
		lr, ok := rep.(repo.DirectRepository)
		if !ok {
			return errors.Errorf("operation supported only on direct repository")
		}

		return act(ctx, lr)
	}
}

func (c *App) directRepositoryWriteAction(act func(ctx context.Context, rep repo.DirectRepositoryWriter) error) func(ctx *kingpin.ParseContext) error {
	return c.maybeRepositoryAction(assertDirectRepository(func(ctx context.Context, rep repo.DirectRepository) error {
		// nolint:wrapcheck
		return repo.DirectWriteSession(ctx, rep, repo.WriteSessionOptions{
			Purpose:  "directRepositoryWriteAction",
			OnUpload: c.progress.UploadedBytes,
		}, func(dw repo.DirectRepositoryWriter) error { return act(ctx, dw) })
	}), repositoryAccessMode{
		mustBeConnected:    true,
		disableMaintenance: true,
	})
}

func (c *App) directRepositoryReadAction(act func(ctx context.Context, rep repo.DirectRepository) error) func(ctx *kingpin.ParseContext) error {
	return c.maybeRepositoryAction(assertDirectRepository(func(ctx context.Context, rep repo.DirectRepository) error {
		return act(ctx, rep)
	}), repositoryAccessMode{
		mustBeConnected:    true,
		disableMaintenance: true,
	})
}

func (c *App) repositoryReaderAction(act func(ctx context.Context, rep repo.Repository) error) func(ctx *kingpin.ParseContext) error {
	return c.maybeRepositoryAction(func(ctx context.Context, rep repo.Repository) error {
		return act(ctx, rep)
	}, repositoryAccessMode{
		mustBeConnected:    true,
		disableMaintenance: true,
	})
}

func (c *App) repositoryWriterAction(act func(ctx context.Context, rep repo.RepositoryWriter) error) func(ctx *kingpin.ParseContext) error {
	return c.maybeRepositoryAction(func(ctx context.Context, rep repo.Repository) error {
		// nolint:wrapcheck
		return repo.WriteSession(ctx, rep, repo.WriteSessionOptions{
			Purpose:  "repositoryWriterAction",
			OnUpload: c.progress.UploadedBytes,
		}, func(w repo.RepositoryWriter) error {
			return act(ctx, w)
		})
	}, repositoryAccessMode{
		mustBeConnected: true,
	})
}

func (c *App) rootContext() context.Context {
	return c.rootctx
}

type repositoryAccessMode struct {
	mustBeConnected    bool
	disableMaintenance bool
}

func (c *App) maybeRepositoryAction(act func(ctx context.Context, rep repo.Repository) error, mode repositoryAccessMode) func(ctx *kingpin.ParseContext) error {
	return func(kpc *kingpin.ParseContext) error {
		ctx := c.rootContext()

		if err := withProfiling(func() error {
			c.mt.startMemoryTracking(ctx)
			defer c.mt.finishMemoryTracking(ctx)

			if c.metricsListenAddr != "" {
				mux := http.NewServeMux()
				if err := initPrometheus(mux); err != nil {
					return errors.Wrap(err, "unable to initialize prometheus.")
				}

				log(ctx).Infof("starting prometheus metrics on %v", c.metricsListenAddr)
				go http.ListenAndServe(c.metricsListenAddr, mux) // nolint:errcheck
			}

			rep, err := c.openRepository(ctx, mode.mustBeConnected)
			if err != nil && mode.mustBeConnected {
				return errors.Wrap(err, "open repository")
			}

			err = act(ctx, rep)

			if rep != nil && !mode.disableMaintenance {
				if merr := c.maybeRunMaintenance(ctx, rep); merr != nil {
					log(ctx).Errorf("error running maintenance: %v", merr)
				}
			}

			if rep != nil && mode.mustBeConnected {
				if cerr := rep.Close(ctx); cerr != nil {
					return errors.Wrap(cerr, "unable to close repository")
				}
			}

			return err
		}); err != nil {
			// print error in red
			log(ctx).Errorf("ERROR: %v", err.Error())
			c.osExit(1)
		}

		return nil
	}
}

func (c *App) maybeRunMaintenance(ctx context.Context, rep repo.Repository) error {
	if !c.enableAutomaticMaintenance {
		return nil
	}

	if rep.ClientOptions().ReadOnly {
		return nil
	}

	dr, ok := rep.(repo.DirectRepository)
	if !ok {
		return nil
	}

	err := repo.DirectWriteSession(ctx, dr, repo.WriteSessionOptions{
		Purpose:  "maybeRunMaintenance",
		OnUpload: c.progress.UploadedBytes,
	}, func(w repo.DirectRepositoryWriter) error {
		// nolint:wrapcheck
		return snapshotmaintenance.Run(ctx, w, maintenance.ModeAuto, false, maintenance.SafetyFull)
	})

	var noe maintenance.NotOwnedError

	if errors.As(err, &noe) {
		// do not report the NotOwnedError to the user since this is automatic maintenance.
		return nil
	}

	return errors.Wrap(err, "error running maintenance")
}

func (c *App) advancedCommand(ctx context.Context) {
	if c.AdvancedCommands != "enabled" {
		_, _ = errorColor.Fprintf(c.stderrWriter, `
This command could be dangerous or lead to repository corruption when used improperly.

Running this command is not needed for using Kopia. Instead, most users should rely on periodic repository maintenance. See https://kopia.io/docs/advanced/maintenance/ for more information.
To run this command despite the warning, set KOPIA_ADVANCED_COMMANDS=enabled

`)

		c.osExit(1)
	}
}

func init() {
	kingpin.EnableFileExpansion = false
}
