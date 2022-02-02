// Package cli implements command-line commands for the Kopia.
package cli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/fatih/color"
	"github.com/gorilla/mux"
	"github.com/mattn/go-colorable"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/memtrack"
	"github.com/kopia/kopia/internal/passwordpersist"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

var log = logging.Module("kopia/cli")

// nolint:gochecknoglobals
var (
	defaultColor = color.New()
	warningColor = color.New(color.FgYellow)
	errorColor   = color.New(color.FgHiRed)
	noteColor    = color.New(color.FgHiCyan)
)

type textOutput struct {
	svc appServices
}

func (o *textOutput) setup(svc appServices) {
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

	return o.svc.Stderr()
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
	Stderr() io.Writer
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
	getPasswordFromFlags(ctx context.Context, isCreate, allowPersistent bool) (string, error)
	optionsFromFlags(ctx context.Context) *repo.Options
	rootContext() context.Context
}

// App contains per-invocation flags and state of Kopia CLI.
type App struct {
	// global flags
	enableAutomaticMaintenance    bool
	pf                            profileFlags
	mt                            memoryTracker
	progress                      *cliProgress
	initialUpdateCheckDelay       time.Duration
	updateCheckInterval           time.Duration
	updateAvailableNotifyInterval time.Duration
	password                      string
	configPath                    string
	traceStorage                  bool
	metricsListenAddr             string
	enablePProf                   bool
	keyRingEnabled                bool
	persistCredentials            bool
	disableInternalLog            bool
	AdvancedCommands              string

	currentAction   string
	onExitCallbacks []func()

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
	logs        commandLogs

	// testability hooks
	osExit        func(int) // allows replacing os.Exit() with custom code
	stdoutWriter  io.Writer
	stderrWriter  io.Writer
	rootctx       context.Context // nolint:containedctx
	loggerFactory logging.LoggerFactory
}

func (c *App) getProgress() *cliProgress {
	return c.progress
}

func (c *App) stdout() io.Writer {
	return c.stdoutWriter
}

// Stderr returns the stderr writer.
func (c *App) Stderr() io.Writer {
	return c.stderrWriter
}

// SetLoggerFactory sets the logger factory to be used throughout the app.
func (c *App) SetLoggerFactory(loggerForModule logging.LoggerFactory) {
	c.loggerFactory = loggerForModule
}

// RegisterOnExit registers the provided function to run before app exits.
func (c *App) RegisterOnExit(f func()) {
	c.onExitCallbacks = append(c.onExitCallbacks, f)
}

// runOnExit runs all registered on-exit callbacks.
func (c *App) runOnExit() {
	for _, f := range c.onExitCallbacks {
		f()
	}
}

func (c *App) passwordPersistenceStrategy() passwordpersist.Strategy {
	if !c.persistCredentials {
		return passwordpersist.None()
	}

	if c.keyRingEnabled {
		return passwordpersist.Multiple{
			passwordpersist.Keyring(),
			passwordpersist.File(),
		}
	}

	return passwordpersist.File()
}

func (c *App) setup(app *kingpin.Application) {
	app.PreAction(func(pc *kingpin.ParseContext) error {
		if sc := pc.SelectedCommand; sc != nil {
			c.currentAction = sc.FullCommand()
		} else {
			c.currentAction = "unknown-action"
		}

		return nil
	})

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
	app.Flag("config-file", "Specify the config file to use").Default("repository.config").Envar("KOPIA_CONFIG_PATH").StringVar(&c.configPath)
	app.Flag("trace-storage", "Enables tracing of storage operations.").Default("true").Hidden().BoolVar(&c.traceStorage)
	app.Flag("metrics-listen-addr", "Expose Prometheus metrics on a given host:port").Hidden().StringVar(&c.metricsListenAddr)
	app.Flag("enable-pprof", "Expose pprof handlers").Hidden().BoolVar(&c.enablePProf)
	app.Flag("timezone", "Format time according to specified time zone (local, utc, original or time zone name)").Hidden().StringVar(&timeZone)
	app.Flag("password", "Repository password.").Envar("KOPIA_PASSWORD").Short('p').StringVar(&c.password)
	app.Flag("persist-credentials", "Persist credentials").Default("true").Envar("KOPIA_PERSIST_CREDENTIALS_ON_CONNECT").BoolVar(&c.persistCredentials)
	app.Flag("disable-internal-log", "Disable internal log").Hidden().Envar("KOPIA_DISABLE_INTERNAL_LOG").BoolVar(&c.disableInternalLog)
	app.Flag("advanced-commands", "Enable advanced (and potentially dangerous) commands.").Hidden().Envar("KOPIA_ADVANCED_COMMANDS").StringVar(&c.AdvancedCommands)

	c.setupOSSpecificKeychainFlags(app)

	_ = app.Flag("caching", "Enables caching of objects (disable with --no-caching)").Default("true").Hidden().Action(
		deprecatedFlag(c.stderrWriter, "The '--caching' flag is deprecated and has no effect, use 'kopia cache set' instead."),
	).Bool()

	_ = app.Flag("list-caching", "Enables caching of list results (disable with --no-list-caching)").Default("true").Hidden().Action(
		deprecatedFlag(c.stderrWriter, "The '--list-caching' flag is deprecated and has no effect, use 'kopia cache set' instead."),
	).Bool()

	c.mt.setup(app)
	c.pf.setup(app)
	c.progress.setup(c, app)

	c.blob.setup(c, app)
	c.benchmark.setup(c, app)
	c.cache.setup(c, app)
	c.content.setup(c, app)
	c.diff.setup(c, app)
	c.index.setup(c, app)
	c.list.setup(c, app)
	c.logs.setup(c, app)
	c.server.setup(c, app)
	c.session.setup(c, app)
	c.restore.setup(c, app)
	c.show.setup(c, app)
	c.snapshot.setup(c, app)
	c.manifest.setup(c, app)
	c.policy.setup(c, app)
	c.mount.setup(c, app)
	c.maintenance.setup(c, app)
	c.repository.setup(c, app) // nolint:contextcheck
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
		stdoutWriter: colorable.NewColorableStdout(),
		stderrWriter: colorable.NewColorableStderr(),
		rootctx:      context.Background(),
	}
}

// Attach attaches the CLI parser to the application.
func (c *App) Attach(app *kingpin.Application) {
	c.setup(app) // nolint:contextcheck
}

// safetyFlagVar defines c --safety=none|full flag that sets the SafetyParameters.
func safetyFlagVar(cmd *kingpin.CmdClause, result *maintenance.SafetyParameters) {
	var str string

	*result = maintenance.SafetyFull

	safetyByName := map[string]maintenance.SafetyParameters{
		"none": maintenance.SafetyNone,
		"full": maintenance.SafetyFull,
	}

	cmd.Flag("safety", "Safety level").Default("full").PreAction(func(pc *kingpin.ParseContext) error {
		r, ok := safetyByName[str]
		if !ok {
			return errors.Errorf("unhandled safety level")
		}

		*result = r

		return nil
	}).EnumVar(&str, "full", "none")
}

func (c *App) currentActionName() string {
	return c.currentAction
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
			Purpose:  "cli:" + c.currentActionName(),
			OnUpload: c.progress.UploadedBytes,
		}, func(ctx context.Context, dw repo.DirectRepositoryWriter) error { return act(ctx, dw) })
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
			Purpose:  "cli:" + c.currentActionName(),
			OnUpload: c.progress.UploadedBytes,
		}, func(ctx context.Context, w repo.RepositoryWriter) error {
			return act(ctx, w)
		})
	}, repositoryAccessMode{
		mustBeConnected: true,
	})
}

func (c *App) rootContext() context.Context {
	ctx := c.rootctx

	if c.loggerFactory != nil {
		ctx = logging.WithLogger(ctx, c.loggerFactory)
	}

	return ctx
}

type repositoryAccessMode struct {
	mustBeConnected    bool
	disableMaintenance bool
}

func (c *App) maybeRepositoryAction(act func(ctx context.Context, rep repo.Repository) error, mode repositoryAccessMode) func(ctx *kingpin.ParseContext) error {
	return func(kpc *kingpin.ParseContext) error {
		ctx0 := c.rootContext()

		err := c.pf.withProfiling(func() error {
			ctx, finishMemoryTracking := c.mt.startMemoryTracking(ctx0)
			defer finishMemoryTracking()

			defer gather.DumpStats(ctx)

			if c.metricsListenAddr != "" {
				m := mux.NewRouter()
				if err := initPrometheus(m); err != nil {
					return errors.Wrap(err, "unable to initialize prometheus")
				}

				if c.enablePProf {
					m.HandleFunc("/debug/pprof/", pprof.Index)
					m.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
					m.HandleFunc("/debug/pprof/profile", pprof.Profile)
					m.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
					m.HandleFunc("/debug/pprof/trace", pprof.Trace)
				}

				log(ctx).Infof("starting prometheus metrics on %v", c.metricsListenAddr)
				go http.ListenAndServe(c.metricsListenAddr, m) // nolint:errcheck
			}

			memtrack.Dump(ctx, "before openRepository")

			rep, err := c.openRepository(ctx, mode.mustBeConnected)

			memtrack.Dump(ctx, "after openRepository")
			if err != nil && mode.mustBeConnected {
				return errors.Wrap(err, "open repository")
			}

			err = act(ctx, rep)

			if rep != nil && !mode.disableMaintenance {
				memtrack.Dump(ctx, "before auto maintenance")

				if merr := c.maybeRunMaintenance(ctx, rep); merr != nil {
					log(ctx).Errorf("error running maintenance: %v", merr)
				}

				memtrack.Dump(ctx, "after auto maintenance")
			}

			if rep != nil && mode.mustBeConnected {
				memtrack.Dump(ctx, "before close repository")

				if cerr := rep.Close(ctx); cerr != nil {
					return errors.Wrap(cerr, "unable to close repository")
				}

				memtrack.Dump(ctx, "after close repository")
			}

			return err
		})

		c.runOnExit()

		if err != nil {
			// print error in red
			log(ctx0).Errorf("%v", err.Error())
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
	}, func(ctx context.Context, w repo.DirectRepositoryWriter) error {
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
To run this command despite the warning, set --advanced-commands=enabled

`)

		c.osExit(1)
	}
}

func init() {
	kingpin.EnableFileExpansion = false
}
