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

	enableAutomaticMaintenance = app.Flag("auto-maintenance", "Automatic maintenance").Default("true").Hidden().Bool()

	_ = app.Flag("help-full", "Show help for all commands, including hidden").Action(helpFullAction).Bool()

	repositoryCommands  = app.Command("repository", "Commands to manipulate repository.").Alias("repo")
	cacheCommands       = app.Command("cache", "Commands to manipulate local cache").Hidden()
	snapshotCommands    = app.Command("snapshot", "Commands to manipulate snapshots.").Alias("snap")
	policyCommands      = app.Command("policy", "Commands to manipulate snapshotting policies.").Alias("policies")
	serverCommands      = app.Command("server", "Commands to control HTTP API server.")
	manifestCommands    = app.Command("manifest", "Low-level commands to manipulate manifest items.").Hidden()
	contentCommands     = app.Command("content", "Commands to manipulate content in repository.").Alias("contents").Hidden()
	blobCommands        = app.Command("blob", "Commands to manipulate BLOBs.").Hidden()
	indexCommands       = app.Command("index", "Commands to manipulate content index.").Hidden()
	benchmarkCommands   = app.Command("benchmark", "Commands to test performance of algorithms.").Hidden()
	maintenanceCommands = app.Command("maintenance", "Maintenance commands.").Hidden().Alias("gc")
	sessionCommands     = app.Command("session", "Session commands.").Hidden()
	userCommands        = serverCommands.Command("users", "Manager repository users").Alias("user")
	aclCommands         = serverCommands.Command("acl", "Manager server access control list entries")
)

var safetyByName = map[string]maintenance.SafetyParameters{
	"none": maintenance.SafetyNone,
	"full": maintenance.SafetyFull,
}

// safetyFlag defines a --safety=none|full flag that returns SafetyParameters.
func safetyFlag(c *kingpin.CmdClause) *maintenance.SafetyParameters {
	var (
		result = maintenance.SafetyFull
		str    string
	)

	c.Flag("safety", "Safety level").Default("full").PreAction(func(pc *kingpin.ParseContext) error {
		var ok bool
		result, ok = safetyByName[str]
		if !ok {
			return errors.Errorf("unhandled safety level")
		}

		return nil
	}).EnumVar(&str, "full", "none")

	return &result
}

func helpFullAction(ctx *kingpin.ParseContext) error {
	_ = app.UsageForContextWithTemplate(ctx, 0, kingpin.DefaultUsageTemplate)

	os.Exit(0)

	return nil
}

func noRepositoryAction(act func(ctx context.Context) error) func(ctx *kingpin.ParseContext) error {
	return func(_ *kingpin.ParseContext) error {
		return act(rootContext())
	}
}

func serverAction(act func(ctx context.Context, cli *apiclient.KopiaAPIClient) error) func(ctx *kingpin.ParseContext) error {
	return func(_ *kingpin.ParseContext) error {
		opts, err := serverAPIClientOptions()
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

func directRepositoryWriteAction(act func(ctx context.Context, rep repo.DirectRepositoryWriter) error) func(ctx *kingpin.ParseContext) error {
	return maybeRepositoryAction(assertDirectRepository(func(ctx context.Context, rep repo.DirectRepository) error {
		return repo.DirectWriteSession(ctx, rep, repo.WriteSessionOptions{
			Purpose:  "directRepositoryWriteAction",
			OnUpload: progress.UploadedBytes,
		}, func(dw repo.DirectRepositoryWriter) error { return act(ctx, dw) })
	}), repositoryAccessMode{
		mustBeConnected:    true,
		disableMaintenance: true,
	})
}

func directRepositoryReadAction(act func(ctx context.Context, rep repo.DirectRepository) error) func(ctx *kingpin.ParseContext) error {
	return maybeRepositoryAction(assertDirectRepository(func(ctx context.Context, rep repo.DirectRepository) error {
		return act(ctx, rep)
	}), repositoryAccessMode{
		mustBeConnected:    true,
		disableMaintenance: true,
	})
}

func repositoryReaderAction(act func(ctx context.Context, rep repo.Repository) error) func(ctx *kingpin.ParseContext) error {
	return maybeRepositoryAction(func(ctx context.Context, rep repo.Repository) error {
		return act(ctx, rep)
	}, repositoryAccessMode{
		mustBeConnected:    true,
		disableMaintenance: true,
	})
}

func repositoryWriterAction(act func(ctx context.Context, rep repo.RepositoryWriter) error) func(ctx *kingpin.ParseContext) error {
	return maybeRepositoryAction(func(ctx context.Context, rep repo.Repository) error {
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

func maybeRepositoryAction(act func(ctx context.Context, rep repo.Repository) error, mode repositoryAccessMode) func(ctx *kingpin.ParseContext) error {
	return func(kpc *kingpin.ParseContext) error {
		ctx := rootContext()

		if err := withProfiling(func() error {
			startMemoryTracking(ctx)
			defer finishMemoryTracking(ctx)

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
				if merr := maybeRunMaintenance(ctx, rep); merr != nil {
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

func maybeRunMaintenance(ctx context.Context, rep repo.Repository) error {
	if !*enableAutomaticMaintenance {
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
