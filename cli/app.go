// Package cli implements command-line commands for the Kopia.
package cli

import (
	"context"
	"net/http"
	"os"

	"github.com/alecthomas/kingpin"
	"github.com/fatih/color"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/repo"
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

var (
	app = kingpin.New("kopia", "Kopia - Online Backup").Author("http://kopia.github.io/")

	_ = app.Flag("help-full", "Show help for all commands, including hidden").Action(helpFullAction).Bool()
)

// appServices are the methods of *TheApp that command handles are allowed to call.
type appServices interface {
	noRepositoryAction(act func(ctx context.Context) error) func(ctx *kingpin.ParseContext) error
	serverAction(sf *serverClientFlags, act func(ctx context.Context, cli *apiclient.KopiaAPIClient) error) func(ctx *kingpin.ParseContext) error
	directRepositoryWriteAction(act func(ctx context.Context, rep repo.DirectRepositoryWriter) error) func(ctx *kingpin.ParseContext) error
	directRepositoryReadAction(act func(ctx context.Context, rep repo.DirectRepository) error) func(ctx *kingpin.ParseContext) error
	repositoryReaderAction(act func(ctx context.Context, rep repo.Repository) error) func(ctx *kingpin.ParseContext) error
	repositoryWriterAction(act func(ctx context.Context, rep repo.RepositoryWriter) error) func(ctx *kingpin.ParseContext) error
	maybeRepositoryAction(act func(ctx context.Context, rep repo.Repository) error, mode repositoryAccessMode) func(ctx *kingpin.ParseContext) error
}

type TheApp struct {
	// global flags
	enableAutomaticMaintenance bool
	mt                         memoryTracker

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
}

func (c *TheApp) setup(app *kingpin.Application) {
	app.Flag("auto-maintenance", "Automatic maintenance").Default("true").Hidden().BoolVar(&c.enableAutomaticMaintenance)
	c.mt.setup(app)

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

func init() {
	a := &TheApp{}
	a.setup(app)
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

func helpFullAction(ctx *kingpin.ParseContext) error {
	_ = app.UsageForContextWithTemplate(ctx, 0, kingpin.DefaultUsageTemplate)

	os.Exit(0)

	return nil
}

func (c *TheApp) noRepositoryAction(act func(ctx context.Context) error) func(ctx *kingpin.ParseContext) error {
	return func(_ *kingpin.ParseContext) error {
		return act(rootContext())
	}
}

func (c *TheApp) serverAction(sf *serverClientFlags, act func(ctx context.Context, cli *apiclient.KopiaAPIClient) error) func(ctx *kingpin.ParseContext) error {
	return func(_ *kingpin.ParseContext) error {
		opts, err := sf.serverAPIClientOptions()
		if err != nil {
			return errors.Wrap(err, "unable to create API client options")
		}

		apiClient, err := apiclient.NewKopiaAPIClient(opts)
		if err != nil {
			return errors.Wrap(err, "unable to create API client")
		}

		return act(rootContext(), apiClient)
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

func (c *TheApp) directRepositoryWriteAction(act func(ctx context.Context, rep repo.DirectRepositoryWriter) error) func(ctx *kingpin.ParseContext) error {
	return c.maybeRepositoryAction(assertDirectRepository(func(ctx context.Context, rep repo.DirectRepository) error {
		return repo.DirectWriteSession(ctx, rep, repo.WriteSessionOptions{
			Purpose:  "directRepositoryWriteAction",
			OnUpload: progress.UploadedBytes,
		}, func(dw repo.DirectRepositoryWriter) error { return act(ctx, dw) })
	}), repositoryAccessMode{
		mustBeConnected:    true,
		disableMaintenance: true,
	})
}

func (c *TheApp) directRepositoryReadAction(act func(ctx context.Context, rep repo.DirectRepository) error) func(ctx *kingpin.ParseContext) error {
	return c.maybeRepositoryAction(assertDirectRepository(func(ctx context.Context, rep repo.DirectRepository) error {
		return act(ctx, rep)
	}), repositoryAccessMode{
		mustBeConnected:    true,
		disableMaintenance: true,
	})
}

func (c *TheApp) repositoryReaderAction(act func(ctx context.Context, rep repo.Repository) error) func(ctx *kingpin.ParseContext) error {
	return c.maybeRepositoryAction(func(ctx context.Context, rep repo.Repository) error {
		return act(ctx, rep)
	}, repositoryAccessMode{
		mustBeConnected:    true,
		disableMaintenance: true,
	})
}

func (c *TheApp) repositoryWriterAction(act func(ctx context.Context, rep repo.RepositoryWriter) error) func(ctx *kingpin.ParseContext) error {
	return c.maybeRepositoryAction(func(ctx context.Context, rep repo.Repository) error {
		return repo.WriteSession(ctx, rep, repo.WriteSessionOptions{
			Purpose:  "repositoryWriterAction",
			OnUpload: progress.UploadedBytes,
		}, func(w repo.RepositoryWriter) error {
			return act(ctx, w)
		})
	}, repositoryAccessMode{
		mustBeConnected: true,
	})
}

func rootContext() context.Context {
	return context.Background()
}

type repositoryAccessMode struct {
	mustBeConnected    bool
	disableMaintenance bool
}

func (c *TheApp) maybeRepositoryAction(act func(ctx context.Context, rep repo.Repository) error, mode repositoryAccessMode) func(ctx *kingpin.ParseContext) error {
	return func(kpc *kingpin.ParseContext) error {
		ctx := rootContext()

		if err := withProfiling(func() error {
			c.mt.startMemoryTracking(ctx)
			defer c.mt.finishMemoryTracking(ctx)

			if *metricsListenAddr != "" {
				mux := http.NewServeMux()
				if err := initPrometheus(mux); err != nil {
					return errors.Wrap(err, "unable to initialize prometheus.")
				}

				log(ctx).Infof("starting prometheus metrics on %v", *metricsListenAddr)
				go http.ListenAndServe(*metricsListenAddr, mux) // nolint:errcheck
			}

			rep, err := openRepository(ctx, mode.mustBeConnected)
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
			os.Exit(1)
		}

		return nil
	}
}

func (c *TheApp) maybeRunMaintenance(ctx context.Context, rep repo.Repository) error {
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
		OnUpload: progress.UploadedBytes,
	}, func(w repo.DirectRepositoryWriter) error {
		return snapshotmaintenance.Run(ctx, w, maintenance.ModeAuto, false, maintenance.SafetyFull)
	})

	var noe maintenance.NotOwnedError

	if errors.As(err, &noe) {
		// do not report the NotOwnedError to the user since this is automatic maintenance.
		return nil
	}

	return errors.Wrap(err, "error running maintenance")
}

func advancedCommand(ctx context.Context) {
	if os.Getenv("KOPIA_ADVANCED_COMMANDS") != "enabled" {
		log(ctx).Errorf(`
This command could be dangerous or lead to repository corruption when used improperly.

Running this command is not needed for using Kopia. Instead, most users should rely on periodic repository maintenance. See https://kopia.io/docs/advanced/maintenance/ for more information.
To run this command despite the warning, set KOPIA_ADVANCED_COMMANDS=enabled

`)
		os.Exit(1)
	}
}

// App returns an instance of command-line application object.
func App() *kingpin.Application {
	return app
}
