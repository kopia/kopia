// Package cli implements command-line commands for the Kopia.
package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/fatih/color"
	"github.com/mattn/go-colorable"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/passwordpersist"
	"github.com/kopia/kopia/internal/releasable"
	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/notification/notifydata"
	"github.com/kopia/kopia/notification/notifytemplate"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

var log = logging.Module("kopia/cli")

var tracer = otel.Tracer("cli")

//nolint:gochecknoglobals
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
	fmt.Fprintf(o.stdout(), msg, args...) //nolint:errcheck
}

func (o *textOutput) printStderr(msg string, args ...interface{}) {
	fmt.Fprintf(o.stderr(), msg, args...) //nolint:errcheck
}

// appServices are the methods of *App that command handles are allowed to call.
//
//nolint:interfacebloat
type appServices interface {
	noRepositoryAction(act func(ctx context.Context) error) func(ctx *kingpin.ParseContext) error
	serverAction(sf *serverClientFlags, act func(ctx context.Context, cli *apiclient.KopiaAPIClient) error) func(ctx *kingpin.ParseContext) error
	directRepositoryWriteAction(act func(ctx context.Context, rep repo.DirectRepositoryWriter) error) func(ctx *kingpin.ParseContext) error
	directRepositoryReadAction(act func(ctx context.Context, rep repo.DirectRepository) error) func(ctx *kingpin.ParseContext) error
	repositoryReaderAction(act func(ctx context.Context, rep repo.Repository) error) func(ctx *kingpin.ParseContext) error
	repositoryWriterAction(act func(ctx context.Context, rep repo.RepositoryWriter) error) func(ctx *kingpin.ParseContext) error
	repositoryHintAction(act func(ctx context.Context, rep repo.Repository) []string) func() []string
	maybeRepositoryAction(act func(ctx context.Context, rep repo.Repository) error, mode repositoryAccessMode) func(ctx *kingpin.ParseContext) error
	baseActionWithContext(act func(ctx context.Context) error) func(ctx *kingpin.ParseContext) error
	openRepository(ctx context.Context, mustBeConnected bool) (repo.Repository, error)
	advancedCommand(ctx context.Context)
	repositoryConfigFileName() string
	getProgress() *cliProgress
	getRestoreProgress() RestoreProgress
	notificationTemplateOptions() notifytemplate.Options

	stdout() io.Writer
	Stderr() io.Writer
	stdin() io.Reader
	onTerminate(callback func())
	onRepositoryFatalError(callback func(err error))
	enableTestOnlyFlags() bool
	EnvName(s string) string
}

//nolint:interfacebloat
type advancedAppServices interface {
	appServices
	StorageProviderServices

	runConnectCommandWithStorage(ctx context.Context, co *connectOptions, st blob.Storage) error
	runConnectCommandWithStorageAndPassword(ctx context.Context, co *connectOptions, st blob.Storage, password string) error
	openRepository(ctx context.Context, required bool) (repo.Repository, error)
	maybeInitializeUpdateCheck(ctx context.Context, co *connectOptions)
	removeUpdateState()
	passwordPersistenceStrategy() passwordpersist.Strategy
	getPasswordFromFlags(ctx context.Context, isCreate, allowPersistent bool) (string, error)
	optionsFromFlags(ctx context.Context) *repo.Options
	runAppWithContext(command *kingpin.CmdClause, callback func(ctx context.Context) error) error
	enableErrorNotifications() bool
}

// App contains per-invocation flags and state of Kopia CLI.
type App struct {
	// global flags
	enableAutomaticMaintenance    bool
	pf                            profileFlags
	progress                      *cliProgress
	restoreProgress               RestoreProgress
	initialUpdateCheckDelay       time.Duration
	updateCheckInterval           time.Duration
	updateAvailableNotifyInterval time.Duration
	password                      string
	configPath                    string
	traceStorage                  bool
	keyRingEnabled                bool
	persistCredentials            bool
	disableInternalLog            bool
	dumpAllocatorStats            bool
	AdvancedCommands              string
	cliStorageProviders           []StorageProvider
	trackReleasable               []string

	observability       observabilityFlags
	upgradeOwnerID      string
	doNotWaitForUpgrade bool

	errorNotifications string

	currentAction         string
	onExitCallbacks       []func()
	onFatalErrorCallbacks []func(err error)

	// subcommands
	blob         commandBlob
	benchmark    commandBenchmark
	cache        commandCache
	content      commandContent
	diff         commandDiff
	index        commandIndex
	list         commandList
	server       commandServer
	session      commandSession
	policy       commandPolicy
	restore      commandRestore
	show         commandShow
	snapshot     commandSnapshot
	manifest     commandManifest
	mount        commandMount
	maintenance  commandMaintenance
	repository   commandRepository
	logs         commandLogs
	notification commandNotification

	// testability hooks
	testonlyIgnoreMissingRequiredFeatures bool

	isInProcessTest bool
	exitWithError   func(err error) // os.Exit() with 1 or 0 based on err
	stdinReader     io.Reader
	stdoutWriter    io.Writer
	stderrWriter    io.Writer
	rootctx         context.Context //nolint:containedctx
	loggerFactory   logging.LoggerFactory
	simulatedCtrlC  chan bool
	envNamePrefix   string
}

func (c *App) enableTestOnlyFlags() bool {
	return c.isInProcessTest || os.Getenv("KOPIA_TESTONLY_FLAGS") != ""
}

func (c *App) getProgress() *cliProgress {
	return c.progress
}

// SetRestoreProgress is used to set custom restore progress, purposed to be used in tests.
func (c *App) SetRestoreProgress(p RestoreProgress) {
	c.restoreProgress = p
}

func (c *App) getRestoreProgress() RestoreProgress {
	return c.restoreProgress
}

func (c *App) stdin() io.Reader {
	return c.stdinReader
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
		c.exitWithError(nil)
		return nil
	}).Bool()

	app.Flag("auto-maintenance", "Automatic maintenance").Default("true").Hidden().BoolVar(&c.enableAutomaticMaintenance)

	// hidden flags to control auto-update behavior.
	app.Flag("initial-update-check-delay", "Initial delay before first time update check").Default("24h").Hidden().Envar(c.EnvName("KOPIA_INITIAL_UPDATE_CHECK_DELAY")).DurationVar(&c.initialUpdateCheckDelay)
	app.Flag("update-check-interval", "Interval between update checks").Default("168h").Hidden().Envar(c.EnvName("KOPIA_UPDATE_CHECK_INTERVAL")).DurationVar(&c.updateCheckInterval)
	app.Flag("update-available-notify-interval", "Interval between update notifications").Default("1h").Hidden().Envar(c.EnvName("KOPIA_UPDATE_NOTIFY_INTERVAL")).DurationVar(&c.updateAvailableNotifyInterval)
	app.Flag("config-file", "Specify the config file to use").Default("repository.config").Envar(c.EnvName("KOPIA_CONFIG_PATH")).StringVar(&c.configPath)
	app.Flag("trace-storage", "Enables tracing of storage operations.").Default("true").Hidden().BoolVar(&c.traceStorage)
	app.Flag("timezone", "Format time according to specified time zone (local, utc, original or time zone name)").Hidden().StringVar(&timeZone)
	app.Flag("password", "Repository password.").Envar(c.EnvName("KOPIA_PASSWORD")).Short('p').StringVar(&c.password)
	app.Flag("persist-credentials", "Persist credentials").Default("true").Envar(c.EnvName("KOPIA_PERSIST_CREDENTIALS_ON_CONNECT")).BoolVar(&c.persistCredentials)
	app.Flag("disable-internal-log", "Disable internal log").Hidden().Envar(c.EnvName("KOPIA_DISABLE_INTERNAL_LOG")).BoolVar(&c.disableInternalLog)
	app.Flag("advanced-commands", "Enable advanced (and potentially dangerous) commands.").Hidden().Envar(c.EnvName("KOPIA_ADVANCED_COMMANDS")).StringVar(&c.AdvancedCommands)
	app.Flag("track-releasable", "Enable tracking of releasable resources.").Hidden().Envar(c.EnvName("KOPIA_TRACK_RELEASABLE")).StringsVar(&c.trackReleasable)
	app.Flag("dump-allocator-stats", "Dump allocator stats at the end of execution.").Hidden().Envar(c.EnvName("KOPIA_DUMP_ALLOCATOR_STATS")).BoolVar(&c.dumpAllocatorStats)
	app.Flag("upgrade-owner-id", "Repository format upgrade owner-id.").Hidden().Envar(c.EnvName("KOPIA_REPO_UPGRADE_OWNER_ID")).StringVar(&c.upgradeOwnerID)
	app.Flag("upgrade-no-block", "Do not block when repository format upgrade is in progress, instead exit with a message.").Hidden().Default("false").Envar(c.EnvName("KOPIA_REPO_UPGRADE_NO_BLOCK")).BoolVar(&c.doNotWaitForUpgrade)
	app.Flag("error-notifications", "Send notification on errors").Hidden().
		Envar(c.EnvName("KOPIA_SEND_ERROR_NOTIFICATIONS")).
		Default(errorNotificationsNonInteractive).
		EnumVar(&c.errorNotifications, errorNotificationsAlways, errorNotificationsNever, errorNotificationsNonInteractive)

	if c.enableTestOnlyFlags() {
		app.Flag("ignore-missing-required-features", "Open repository despite missing features (VERY DANGEROUS, ONLY FOR TESTING)").Hidden().BoolVar(&c.testonlyIgnoreMissingRequiredFeatures)
	}

	c.observability.setup(c, app)

	c.setupOSSpecificKeychainFlags(c, app)

	_ = app.Flag("caching", "Enables caching of objects (disable with --no-caching)").Default("true").Hidden().Action(
		deprecatedFlag(c.stderrWriter, "The '--caching' flag is deprecated and has no effect, use 'kopia cache set' instead."),
	).Bool()

	_ = app.Flag("list-caching", "Enables caching of list results (disable with --no-list-caching)").Default("true").Hidden().Action(
		deprecatedFlag(c.stderrWriter, "The '--list-caching' flag is deprecated and has no effect, use 'kopia cache set' instead."),
	).Bool()

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
	c.notification.setup(c, app)
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
		cliStorageProviders: []StorageProvider{
			{"from-config", "the provided configuration file", func() StorageFlags { return &storageFromConfigFlags{} }},

			{"azure", "an Azure blob storage", func() StorageFlags { return &storageAzureFlags{} }},
			{"b2", "a B2 bucket", func() StorageFlags { return &storageB2Flags{} }},
			{"filesystem", "a filesystem", func() StorageFlags { return &storageFilesystemFlags{} }},
			{"gcs", "a Google Cloud Storage bucket", func() StorageFlags { return &storageGCSFlags{} }},
			{"gdrive", "a Google Drive folder", func() StorageFlags { return &storageGDriveFlags{} }},

			{"rclone", "a rclone-based provided", func() StorageFlags { return &storageRcloneFlags{} }},
			{"s3", "an S3 bucket", func() StorageFlags { return &storageS3Flags{} }},
			{"sftp", "an SFTP storage", func() StorageFlags { return &storageSFTPFlags{} }},
			{"webdav", "a WebDAV storage", func() StorageFlags { return &storageWebDAVFlags{} }},
		},

		// testability hooks
		exitWithError: func(err error) {
			if err != nil {
				os.Exit(1)
			}

			os.Exit(0)
		},
		stdoutWriter: colorable.NewColorableStdout(),
		stderrWriter: colorable.NewColorableStderr(),
		stdinReader:  os.Stdin,
		rootctx:      context.Background(),
	}
}

// SetEnvNamePrefixForTesting sets the name prefix to be used for all environment variable names for testing.
func (c *App) SetEnvNamePrefixForTesting(prefix string) {
	c.envNamePrefix = prefix
}

// EnvName overrides the provided environment variable name for testability.
func (c *App) EnvName(n string) string {
	return c.envNamePrefix + n
}

// Attach attaches the CLI parser to the application.
func (c *App) Attach(app *kingpin.Application) {
	c.setup(app)
}

// safetyFlagVar defines c --safety=none|full flag that sets the SafetyParameters.
func safetyFlagVar(cmd *kingpin.CmdClause, result *maintenance.SafetyParameters) {
	var str string

	*result = maintenance.SafetyFull

	safetyByName := map[string]maintenance.SafetyParameters{
		"none": maintenance.SafetyNone,
		"full": maintenance.SafetyFull,
	}

	cmd.Flag("safety", "Safety level").Default("full").PreAction(func(_ *kingpin.ParseContext) error {
		r, ok := safetyByName[str]
		if !ok {
			return errors.New("unhandled safety level")
		}

		*result = r

		return nil
	}).EnumVar(&str, "full", "none")
}

func (c *App) currentActionName() string {
	return c.currentAction
}

func (c *App) noRepositoryAction(act func(ctx context.Context) error) func(ctx *kingpin.ParseContext) error {
	return func(kpc *kingpin.ParseContext) error {
		return c.runAppWithContext(kpc.SelectedCommand, func(ctx context.Context) error {
			return c.pf.withProfiling(func() error {
				if c.dumpAllocatorStats {
					defer gather.DumpStats(ctx)
				}

				return act(ctx)
			})
		})
	}
}

func (c *App) serverAction(sf *serverClientFlags, act func(ctx context.Context, cli *apiclient.KopiaAPIClient) error) func(ctx *kingpin.ParseContext) error {
	return func(kpc *kingpin.ParseContext) error {
		opts, err := sf.serverAPIClientOptions()
		if err != nil {
			return errors.Wrap(err, "unable to create API client options")
		}

		apiClient, err := apiclient.NewKopiaAPIClient(opts)
		if err != nil {
			return errors.Wrap(err, "unable to create API client")
		}

		return c.runAppWithContext(kpc.SelectedCommand, func(ctx context.Context) error {
			return act(ctx, apiClient)
		})
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
			return errors.New("operation supported only on direct repository")
		}

		return act(ctx, lr)
	}
}

func (c *App) directRepositoryWriteAction(act func(ctx context.Context, rep repo.DirectRepositoryWriter) error) func(ctx *kingpin.ParseContext) error {
	return c.maybeRepositoryAction(assertDirectRepository(func(ctx context.Context, rep repo.DirectRepository) error {
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

func (c *App) runAppWithContext(command *kingpin.CmdClause, cb func(ctx context.Context) error) error {
	ctx := c.rootctx

	if c.loggerFactory != nil {
		ctx = logging.WithLogger(ctx, c.loggerFactory)
	}

	for _, r := range c.trackReleasable {
		releasable.EnableTracking(releasable.ItemKind(r))
	}

	if err := c.observability.startMetrics(ctx); err != nil {
		return errors.Wrap(err, "unable to start metrics")
	}

	err := func() error {
		if command == nil {
			defer c.runOnExit()

			return cb(ctx)
		}

		tctx, span := tracer.Start(ctx, command.FullCommand(), trace.WithSpanKind(trace.SpanKindClient))
		defer span.End()

		defer c.runOnExit()

		return cb(tctx)
	}()

	c.observability.stopMetrics(ctx)

	if err != nil {
		// print error in red
		log(ctx).Errorf("%v", err.Error())
		c.exitWithError(err)
	}

	if len(c.trackReleasable) > 0 {
		if err := releasable.Verify(); err != nil {
			log(ctx).Warnf("%v", err.Error())
			c.exitWithError(err)
		}
	}

	return nil
}

type repositoryAccessMode struct {
	mustBeConnected    bool
	disableMaintenance bool
}

func (c *App) baseActionWithContext(act func(ctx context.Context) error) func(ctx *kingpin.ParseContext) error {
	return func(kpc *kingpin.ParseContext) error {
		return c.runAppWithContext(kpc.SelectedCommand, func(ctx context.Context) error {
			return c.pf.withProfiling(func() error {
				if c.dumpAllocatorStats {
					defer gather.DumpStats(ctx)
				}

				return act(ctx)
			})
		})
	}
}

func (c *App) maybeRepositoryAction(act func(ctx context.Context, rep repo.Repository) error, mode repositoryAccessMode) func(ctx *kingpin.ParseContext) error {
	return c.baseActionWithContext(func(ctx context.Context) error {
		rep, err := c.openRepository(ctx, mode.mustBeConnected)
		if err != nil && mode.mustBeConnected {
			return errors.Wrap(err, "open repository")
		}

		t0 := clock.Now()

		err = act(ctx, rep)

		if rep != nil && err == nil && !mode.disableMaintenance {
			if merr := c.maybeRunMaintenance(ctx, rep); merr != nil {
				log(ctx).Errorf("error running maintenance: %v", merr)
			}
		}

		if err != nil && c.enableErrorNotifications() && rep != nil {
			notification.Send(ctx, rep, "generic-error", notifydata.NewErrorInfo(
				c.currentActionName(),
				c.currentActionName(),
				t0,
				clock.Now(),
				err), notification.SeverityError,
				c.notificationTemplateOptions(),
			)
		}

		if rep != nil && mode.mustBeConnected {
			if cerr := rep.Close(ctx); cerr != nil {
				return errors.Wrap(cerr, "unable to close repository")
			}
		}

		return err
	})
}

func (c *App) repositoryHintAction(act func(ctx context.Context, rep repo.Repository) []string) func() []string {
	return func() []string {
		var result []string

		//nolint:errcheck
		c.runAppWithContext(nil, func(ctx context.Context) error {
			rep, err := c.openRepository(ctx, true)
			if err != nil {
				return nil
			}

			defer rep.Close(ctx) //nolint:errcheck

			result = act(ctx, rep)

			return nil
		})

		return result
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

		c.exitWithError(errors.New("advanced commands are disabled"))
	}
}

func (c *App) notificationTemplateOptions() notifytemplate.Options {
	// perhaps make this configurable in the future
	return notifytemplate.DefaultOptions
}

func init() {
	kingpin.EnableFileExpansion = false
}
